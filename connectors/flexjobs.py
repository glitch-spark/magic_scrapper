"""
FlexJobs connector.

Fetches https://www.flexjobs.com via the homepage search (``/search``).
There is no public RSS/API; listings sit in Next.js ``__NEXT_DATA__`` after
login. Apply/company details are subscription-gated.

Strategy
--------
1. Playwright login with ``FLEXJOBS_EMAIL`` / ``FLEXJOBS_PASSWORD`` (requests
   TLS to this host stalls on this machine). Missing credentials → skip.
2. Search engineering keywords with ``sort=date``. Official UI sort-by-date
   is treated as newest-first: stop when every dated job on a page is older
   than the source age window (30 days on first ingest, then 3). Mixed dates
   on a page do not stop the pager.
3. Parse ``props.pageProps.jobsData.jobs.results``. Keep engineering titles;
   skip expired and stale postings. Skip already-seen listing URLs.
4. Store the FlexJobs job URL. Scoring caps this source at review.
"""
from __future__ import annotations

import json
import os
import re
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlencode, urljoin

from dateutil import parser as dateutil_parser
from dotenv import load_dotenv

from connectors.base import BaseConnector
from utils.alerts import ScrapeHalted, halt
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff
from utils.job_store import known_job_urls, remember_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

load_dotenv()

logger = setup_logger("flexjobs_connector")

BASE_URL = "https://www.flexjobs.com"
LOGIN_URL = "https://www.flexjobs.com/login"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
_FETCH_DELAY = 0.4
_NAV_TIMEOUT_MS = 45_000
_NEXT_DATA_RE = re.compile(
    r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)

# Homepage search box queries. ``sort=date`` + stale-page stop limits crawl size.
_SEARCH_TERMS = (
    "software engineer",
    "python",
    "devops",
    "data engineer",
)

_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}


class FlexJobsConnector(BaseConnector):
    def __init__(self):
        self.source_name = "flexjobs"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        email, password = _credentials()
        if not email or not password:
            logger.error(
                "FLEXJOBS_EMAIL / FLEXJOBS_PASSWORD not set in .env — skipping"
            )
            return []

        logger.info("Fetching jobs from flexjobs.com search…")
        cutoff = job_age_cutoff(self.source_name)
        known = known_job_urls(self.source_name)
        all_jobs: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        remembered: list[str] = []

        try:
            with _browser_session(email, password) as fetch_html:
                if fetch_html is None:
                    return []
                for term in _SEARCH_TERMS:
                    _search_term(
                        term,
                        fetch_html,
                        cutoff,
                        known,
                        seen_ids,
                        all_jobs,
                        remembered,
                        on_job=self._emit,
                    )
        except ScrapeHalted:
            raise
        except Exception as e:
            logger.error(f"Error fetching jobs from flexjobs: {e}")
            logger.debug(traceback.format_exc())

        if remembered:
            remember_listing_urls(self.source_name, remembered)

        logger.info(f"Successfully fetched {len(all_jobs)} jobs from flexjobs")
        return all_jobs

    def normalize(self, raw_job: dict[str, Any]) -> dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        location = raw_job.get("location") or "Remote"

        return {
            "external_id": raw_job.get("id") or url,
            "source": self.source_name,
            "company": raw_job.get("company", "Unknown"),
            "title": raw_job.get("title", ""),
            "location": location,
            "raw_location_text": location,
            "description": description,
            "description_text": clean_description(description),
            "url": url,
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name


def _credentials() -> tuple[str, str]:
    load_dotenv()
    return (
        os.getenv("FLEXJOBS_EMAIL", "").strip(),
        os.getenv("FLEXJOBS_PASSWORD", "").strip(),
    )


def _search_url(term: str, page: int) -> str:
    params: dict[str, str] = {
        "searchkeyword": term,
        "sort": "date",
    }
    if page > 1:
        params["page"] = str(page)
    return f"{BASE_URL}/search?{urlencode(params)}"


def _search_term(
    term: str,
    fetch_html: Callable[[str], str],
    cutoff: datetime,
    known: set[str],
    seen_ids: set[str],
    all_jobs: list[dict[str, Any]],
    remembered: list[str],
    on_job=None,
) -> None:
    page = 1
    total_pages = 1
    prev_ids: set[str] | None = None
    while page <= total_pages:
        html = fetch_html(_search_url(term, page))
        raw_items, reported_pages = _extract_listing_page(html)
        if page == 1:
            total_pages = max(reported_pages, 1)
        if not raw_items:
            break

        page_ids = {
            str(item.get("id") or item.get("slug") or "")
            for item in raw_items
            if item.get("id") or item.get("slug")
        }
        if prev_ids is not None and page_ids and page_ids == prev_ids:
            logger.info(f"FlexJobs '{term}' page {page} repeated prior ids — stopping")
            break
        prev_ids = page_ids

        new_on_page = 0
        dated: list[datetime] = []
        for item in raw_items:
            posted = _parse_dt(item.get("postedDate") or item.get("createdOn"))
            if posted:
                dated.append(posted)
            parsed = _parse_raw_job(item, cutoff)
            if not parsed:
                continue
            remembered.append(parsed["url"])
            if parsed["id"] in seen_ids:
                continue
            if _norm_url(parsed["url"]) in known:
                continue
            seen_ids.add(parsed["id"])
            all_jobs.append(parsed)
            if on_job:
                on_job(parsed)
            new_on_page += 1

        logger.info(
            f"FlexJobs '{term}' page {page}/{total_pages}: "
            f"{len(raw_items)} listings, {new_on_page} kept (total {len(all_jobs)})"
        )

        # sort=date is newest-first: a fully stale page means later pages are older.
        if dated and all(dt < cutoff for dt in dated):
            logger.info(f"FlexJobs '{term}' page {page} is fully stale — stopping")
            break

        page += 1
        if page <= total_pages:
            time.sleep(_FETCH_DELAY)


def _launch_browser(pw: Any) -> Any:
    args = ["--disable-http2"]
    try:
        return pw.chromium.launch(headless=True, channel="chrome", args=args)
    except Exception:
        return pw.chromium.launch(headless=True, args=args)


def _login(page: Any, email: str, password: str) -> bool:
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
    if page.locator("#email").count() == 0 or page.locator("#password").count() == 0:
        logger.error("FlexJobs login form not found")
        return False
    if page.locator("iframe[src*='recaptcha'], iframe[src*='hcaptcha']").count():
        logger.error("FlexJobs login requires captcha — cannot continue headless")
        halt(
            "login_halt",
            "FlexJobs login requires captcha. Sign in manually, then rerun the fetch.",
            source="flexjobs",
        )

    page.fill("#email", email)
    page.fill("#password", password)
    page.click("#login-submit")
    page.wait_for_load_state("domcontentloaded", timeout=_NAV_TIMEOUT_MS)

    for _ in range(20):
        url = (page.url or "").lower()
        cookies = {c.get("name"): c.get("value") for c in page.context.cookies()}
        if cookies.get("authsigninstate") == "1" or cookies.get("Auth"):
            return True
        if "/login" not in url:
            return True
        props = _page_props(page.content())
        if props.get("isLoggedIn") is True:
            return True
        page.wait_for_timeout(500)

    logger.error("FlexJobs login did not succeed")
    return False


@contextmanager
def _browser_session(email: str, password: str):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = _launch_browser(pw)
        context = None
        try:
            context = browser.new_context(user_agent=_UA)
            page = context.new_page()
            if not _login(page, email, password):
                yield None
                return

            def fetch_html(url: str) -> str:
                resp = page.goto(
                    url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS
                )
                if resp is not None and resp.status >= 400:
                    logger.warning(f"FlexJobs HTTP {resp.status} on search page")
                    return ""
                return page.content()

            yield fetch_html
        finally:
            if context is not None:
                context.close()
            browser.close()


def _page_props(html: str) -> dict[str, Any]:
    match = _NEXT_DATA_RE.search(html or "")
    if not match:
        return {}
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    props = (data.get("props") or {}).get("pageProps") or {}
    return props if isinstance(props, dict) else {}


def _extract_listing_page(html: str) -> tuple[list[dict[str, Any]], int]:
    """Return (job dicts, totalPages) from search ``__NEXT_DATA__``."""
    blob = _jobs_blob(_page_props(html))
    if blob is None:
        return [], 1

    results = blob.get("results")
    jobs = [item for item in results if isinstance(item, dict)] if isinstance(results, list) else []

    try:
        total_pages = int(
            blob.get("totalPages")
            or blob.get("total_pages")
            or (blob.get("paging") or {}).get("totalPages")
            or 1
        )
    except (TypeError, ValueError, AttributeError):
        total_pages = 1
    return jobs, max(total_pages, 1)


def _jobs_blob(page_props: dict[str, Any]) -> dict[str, Any] | None:
    jobs_data = page_props.get("jobsData")
    if isinstance(jobs_data, dict):
        jobs = jobs_data.get("jobs")
        if isinstance(jobs, dict) and isinstance(jobs.get("results"), list):
            return jobs
        if isinstance(jobs, list):
            return {"results": jobs, "totalPages": 1}
        if isinstance(jobs_data.get("results"), list):
            return jobs_data
    if isinstance(jobs_data, list):
        return {"results": jobs_data, "totalPages": 1}
    pagination = (page_props.get("data") or {}).get("jobsListWithPagination")
    if isinstance(pagination, dict) and isinstance(pagination.get("results"), list):
        return pagination
    return None


def _extract_listing_jobs(html: str) -> list[dict[str, Any]]:
    jobs, _ = _extract_listing_page(html)
    return jobs


def _is_engineering_title(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in _ENGINEERING_KEYWORDS)


def _company_name(company: Any) -> str:
    if isinstance(company, dict):
        return (company.get("name") or "").strip() or "Unknown"
    if isinstance(company, str) and company.strip():
        return company.strip()
    return "Unknown"


def _stringify_part(value: Any) -> str:
    if isinstance(value, dict):
        parts = [
            value.get("addressLocality") or value.get("city") or "",
            value.get("addressRegion") or value.get("region") or value.get("state") or "",
            value.get("addressCountry") or value.get("country") or "",
            value.get("name") or "",
        ]
        return ", ".join(str(p).strip() for p in parts if p)
    if value is None:
        return ""
    return str(value).strip()


def _location_text(item: dict[str, Any]) -> str:
    for key in ("jobLocations", "allowedCandidateLocation", "locations"):
        val = item.get(key)
        if isinstance(val, list):
            names = [_stringify_part(v) for v in val]
            names = [n for n in names if n]
            if names:
                return ", ".join(names)
        text = _stringify_part(val)
        if text:
            return text
    remote = item.get("remoteOptions")
    if isinstance(remote, list) and remote:
        names = [_stringify_part(v) for v in remote if v]
        if names:
            return ", ".join(names)
    text = _stringify_part(remote)
    if text:
        return text
    return "Remote"


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = dateutil_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _norm_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _job_url(item: dict[str, Any], job_id: str) -> str:
    slug = (item.get("slug") or "").strip()
    if slug:
        return urljoin(BASE_URL + "/", f"jobs/{slug}")
    return urljoin(BASE_URL + "/", f"HostedJob.aspx?id={job_id}")


def _parse_raw_job(item: dict[str, Any], cutoff: datetime) -> dict[str, Any] | None:
    title = (item.get("title") or "").strip()
    if not title or not _is_engineering_title(title):
        return None

    expire_on = _parse_dt(item.get("expireOn"))
    if expire_on and expire_on < datetime.now(tz=timezone.utc):
        return None

    posted_date = _parse_dt(item.get("postedDate") or item.get("createdOn"))
    if posted_date and posted_date < cutoff:
        return None

    slug = (item.get("slug") or "").strip()
    job_id = str(item.get("id") or slug or title[:80])
    url = _job_url(item, job_id)
    description = (item.get("description") or item.get("jobSummary") or "").strip()

    return {
        "id": job_id,
        "title": title,
        "company": _company_name(item.get("company")),
        "url": url,
        "description": description,
        "location": _location_text(item),
        "posted_date": posted_date,
    }
