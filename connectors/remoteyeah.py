"""
RemoteYeah connector.

Guest RSS at the pasted listing path + ``.xml`` (no login). The HTML
listing is a large SSR page; prefer the feed. ``robots.txt`` allows all.

``pubDate`` is newest-first — stop at the first job older than
``max_job_age_days``. Feed XML can be ill-formed (unescaped ``&`` in
``atom:link``); parse ``<item>`` blocks with regex. Engineering title
filter (path already lead/mid/senior + US engineering slugs, but
leaks DevRel / marketing). ``location`` from the description
``Locations:`` bullet (string). Skip known listing URLs. Skip detail
HTTP. Guest POST to ``/jobs/…/apply`` 302s to login. When
``REMOTEYEAH_CHROME_USER_DATA`` is set (OS Chrome profile for RemoteYeah
only, logged into remoteyeah.com), Playwright POSTs that apply URL with the
listing CSRF token and stores the off-site ``Location`` without loading
the ATS page. Login / on-site apply keep the listing URL.
"""
from __future__ import annotations

import html as html_lib
import os
import re
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlsplit, urlunsplit

import requests
from dateutil import parser as dateutil_parser
from dotenv import load_dotenv

from connectors.base import BaseConnector
from utils.alerts import ScrapeHalted, halt
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_inclusion import exclusion_reason, load_candidate_profile
from utils.job_store import (
    jobs_with_url_host,
    remember_listing_urls,
    unseen_listing_urls,
    update_job_url,
)
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

load_dotenv()

logger = setup_logger("remoteyeah_connector")

BASE_URL = "https://remoteyeah.com"
# Pasted path: lead/mid/senior + engineering slugs + United States.
LISTING_PATH = (
    "/remote-lead+mid-level+senior+staff+ai-native-engineer+android-developer+"
    "artificial-intelligence-engineer+backend-engineer+big-data-engineer+"
    "cloud-architect+cloud-engineer+cross-platform-mobile-developer+"
    "data-analyst+data-engineer+database-administrator+database-engineer+"
    "devops-engineer+frontend-engineer+full-stack-engineer+game-developer+"
    "game-engineer+infrastructure-engineer+ios-developer+"
    "machine-learning-engineer+machine-learning-ops-engineer+"
    "platform-engineer+prompt-engineer+qa-engineer+"
    "site-reliability-engineer+software-architect+software-engineer+"
    "solution-architect+solutions-engineer+systems-engineer+web-developer-"
    "jobs-in-united-states"
)
FEED_URL = f"{BASE_URL}{LISTING_PATH}.xml"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml,application/xml,text/xml,*/*;q=0.8",
}
_FEED_TIMEOUT = 40
_RETRIES = 3
_RETRY_DELAY = 1.5
_NAV_TIMEOUT_MS = 30_000
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_REDIRECT_HOPS = 8
_TOKEN_RE = re.compile(
    r'name=["\']_token["\'][^>]*value=["\']([^"\']+)["\']'
    r'|value=["\']([^"\']+)["\'][^>]*name=["\']_token["\']',
    re.I,
)
_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data analyst", "machine learning", "ml ", " ml",
    "ai ", " ai", "ai-native", "ai native", "mlops", "python", "typescript",
    "golang", "rust", "java", "deep learning", "llm ", " llm",
    "artificial intelligence", "agentic", "rag", "android", "ios",
    "big data", "cloud architect", "cloud engineer", "cross-platform",
    "mobile engineer", "mobile developer", "database", "dba",
    "game engineer", "game developer", "infrastructure", "platform engineer",
    "prompt", "qa ", " qa", "sdet", "quality assurance",
    "solution architect", "solutions architect", "solutions engineer",
    "systems engineer", "system engineer", "web developer", "web engineer",
}
_ITEM_RE = re.compile(r"<item>(.*?)</item>", re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.I | re.S)
_LOC_RE = re.compile(r"Locations?:\s*([^<]+)", re.I)
_EMP_RE = re.compile(r"Employments?:\s*([^<]+)", re.I)


def _chrome_user_data_dir() -> str:
    load_dotenv()
    raw = os.getenv("REMOTEYEAH_CHROME_USER_DATA", "").strip().strip('"').strip("'")
    return os.path.normpath(raw) if raw else ""


def _chrome_profile_directory() -> str:
    return os.getenv("REMOTEYEAH_CHROME_PROFILE", "").strip()


def _headless() -> bool:
    return os.getenv("REMOTEYEAH_HEADLESS", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _host(url: str) -> str:
    host = urlparse(url or "").netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _offsite_apply_url(apply_url: Any) -> str:
    url = _strip_tracking(str(apply_url or "").strip())
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return ""
    host = _host(url)
    if not host or host == "remoteyeah.com" or host.endswith(".remoteyeah.com"):
        return ""
    return url


def _csrf_token(html: str) -> str:
    match = _TOKEN_RE.search(html or "")
    if not match:
        return ""
    return (match.group(1) or match.group(2) or "").strip()


def _apply_post_url(listing_url: str) -> str:
    path = (listing_url or "").split("?")[0].rstrip("/")
    if not path:
        return ""
    return f"{path}/apply"


def _response_location(response: Any) -> str:
    headers = getattr(response, "headers", None) or {}
    if hasattr(headers, "get"):
        loc = headers.get("location") or headers.get("Location") or ""
    else:
        loc = ""
    return str(loc).strip()


def _is_login_url(url: str) -> bool:
    path = urlparse(url or "").path.lower()
    return path.rstrip("/").endswith("/login") or "/login" in path


def _follow_apply_post_headers(
    context: Any, listing_url: str, apply_url: str, token: str
) -> str:
    """POST /jobs/…/apply with CSRF token; read Location. Do not load the ATS."""
    request = getattr(context, "request", None)
    if request is None or not apply_url or not token:
        return ""
    current = apply_url
    method = "post"
    for _ in range(_MAX_REDIRECT_HOPS):
        try:
            if method == "post":
                resp = request.post(
                    current,
                    form={"_token": token},
                    max_redirects=0,
                    timeout=_NAV_TIMEOUT_MS,
                    fail_on_status_code=False,
                    headers={"Referer": listing_url, "Origin": BASE_URL},
                )
            else:
                resp = request.get(
                    current,
                    max_redirects=0,
                    timeout=_NAV_TIMEOUT_MS,
                    fail_on_status_code=False,
                    headers={"Referer": listing_url},
                )
        except Exception as e:
            logger.info(f"remoteyeah apply header request failed: {e}")
            return ""
        loc = _response_location(resp)
        if loc:
            nxt = urljoin(current, loc)
            if _is_login_url(nxt):
                return ""
            offsite = _offsite_apply_url(nxt)
            if offsite:
                return offsite
            host = _host(nxt)
            if host == "remoteyeah.com" or host.endswith(".remoteyeah.com"):
                current = nxt
                method = "get"
                continue
            return ""
        status = getattr(resp, "status", 0)
        final = getattr(resp, "url", "") or current
        if status in _REDIRECT_STATUSES or _is_login_url(final):
            return ""
        return _offsite_apply_url(final)
    return ""


class _ApplySession:
    def __init__(self):
        self.resolved = 0
        self._cm: Any = None
        self._pair: tuple[Any, Any] | None = None
        self._failed = False

    def using_chrome(self) -> bool:
        return bool(_chrome_user_data_dir()) and not self._failed

    def open(self) -> bool:
        if not self.using_chrome():
            return False
        return self._ensure_browser() is not None

    def resolve(self, job: dict[str, Any]) -> str:
        if not self.using_chrome():
            return ""
        listing = job.get("listing_url") or ""
        if not listing:
            return ""
        pair = self._ensure_browser()
        if pair is None:
            return ""
        self.resolved += 1
        context, page = pair
        try:
            found = _resolve_apply_on_page(context, page, listing)
        except Exception as e:
            logger.info(
                f"remoteyeah apply capture failed for {job.get('id')}: {e}"
            )
            return ""
        if found:
            logger.info(f"remoteyeah apply URL {job.get('id')}: {found}")
        else:
            logger.info(
                f"remoteyeah apply capture kept listing for {job.get('id')}"
            )
        return found

    def _ensure_browser(self) -> tuple[Any, Any] | None:
        if self._pair is not None:
            return self._pair
        try:
            self._cm = _browser_session()
            self._pair = self._cm.__enter__()
        except ScrapeHalted:
            raise
        except Exception as e:
            logger.warning(f"remoteyeah Chrome profile failed to launch: {e}")
            halt(
                "browser",
                f"remoteyeah Chrome profile failed to launch: {e}",
                source="remoteyeah",
            )
            self._cm = None
            self._pair = None
            return None
        return self._pair

    def close(self) -> None:
        if self._cm is None:
            return
        try:
            self._cm.__exit__(None, None, None)
        except Exception:
            pass
        self._cm = None
        self._pair = None


@contextmanager
def _browser_session():
    from playwright.sync_api import sync_playwright

    user_data = _chrome_user_data_dir()
    profile = _chrome_profile_directory()
    headless = _headless()
    args = ["--disable-http2", "--disable-blink-features=AutomationControlled"]
    if profile:
        args.append(f"--profile-directory={profile}")
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
    with sync_playwright() as pw:
        context = None
        try:
            try:
                context = pw.chromium.launch_persistent_context(
                    user_data,
                    channel="chrome",
                    headless=headless,
                    args=args,
                    locale="en-US",
                    user_agent=ua,
                )
            except Exception:
                context = pw.chromium.launch_persistent_context(
                    user_data,
                    headless=headless,
                    args=args,
                    locale="en-US",
                    user_agent=ua,
                )
            page = context.pages[0] if context.pages else context.new_page()
            logger.info(
                "remoteyeah apply capture using OS Chrome "
                f"user-data-dir={user_data} headless={headless}"
            )
            yield context, page
        finally:
            if context is not None:
                context.close()


def _resolve_apply_on_page(context: Any, page: Any, listing_url: str) -> str:
    page.goto(listing_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
    html = page.content() or ""
    token = _csrf_token(html)
    apply_url = _apply_post_url(listing_url)
    if not token or not apply_url:
        logger.info(f"remoteyeah no apply CSRF token ({listing_url})")
        return ""
    found = _follow_apply_post_headers(context, listing_url, apply_url, token)
    if not found:
        logger.info(f"remoteyeah apply POST stayed on remoteyeah.com ({listing_url})")
    return found


class RemoteYeahConnector(BaseConnector):
    def __init__(self):
        self.source_name = "remoteyeah"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            "Fetching jobs from RemoteYeah RSS "
            f"(age_days={age_days}; cutoff={cutoff.isoformat()}; "
            "newest-first, stop at first stale job)…"
        )
        kept: list[dict[str, Any]] = []
        apply_session = _ApplySession()
        try:
            if apply_session.using_chrome():
                logger.info(
                    "remoteyeah opening OS Chrome for apply capture "
                    f"({_chrome_user_data_dir()})"
                )
                if not apply_session.open():
                    halt(
                        "browser",
                        "remoteyeah Chrome failed to launch. "
                        "Close Chrome if the profile is locked, then rerun the fetch.",
                        source="remoteyeah",
                    )
                else:
                    self._recapture_stored_apply_urls(apply_session)
            else:
                logger.info(
                    "REMOTEYEAH_CHROME_USER_DATA unset — storing remoteyeah.com listing URLs"
                )
            body = _fetch_feed()
            if not body:
                logger.info("Successfully fetched 0 jobs from remoteyeah")
                return []
            items = _ITEM_RE.findall(body)
            page_jobs: list[dict[str, Any]] = []
            seen_ids: set[str] = set()
            for block in items:
                raw = _parse_item(block)
                if not raw:
                    continue
                posted = raw.get("posted_date")
                if posted is None or posted < cutoff:
                    logger.info(
                        "remoteyeah first stale job "
                        f"(posted={posted}, cutoff={cutoff.isoformat()}) "
                        "— stopping newest-first walk"
                    )
                    break
                if not _is_engineering_title(raw["title"]):
                    continue
                if raw["id"] in seen_ids:
                    continue
                seen_ids.add(raw["id"])
                page_jobs.append(raw)
            logger.info(
                f"remoteyeah feed: {len(items)} items, "
                f"{len(page_jobs)} engineering in-window"
            )
            self._emit_jobs(page_jobs, kept, apply_session)
        except ScrapeHalted:
            raise
        except Exception as e:
            logger.error(f"Error fetching jobs from RemoteYeah: {e}")
            logger.debug(traceback.format_exc())
        finally:
            if apply_session.using_chrome() and apply_session.resolved == 0:
                logger.info(
                    "remoteyeah Chrome had no Apply POSTs this run "
                    "(no unseen eligible listings and no stored remoteyeah.com URLs)"
                )
            apply_session.close()
        logger.info(f"Successfully fetched {len(kept)} jobs from remoteyeah")
        return kept

    def _recapture_stored_apply_urls(self, apply_session: "_ApplySession") -> None:
        targets = jobs_with_url_host(self.source_name, "remoteyeah.com")
        if not targets:
            logger.info("remoteyeah no stored listing URLs needing apply capture")
            return
        logger.info(
            f"remoteyeah recapturing Apply URLs for {len(targets)} stored jobs"
        )
        updated = 0
        for external_id, listing_url in targets:
            found = apply_session.resolve({
                "id": external_id,
                "listing_url": listing_url,
            })
            if not found:
                continue
            if update_job_url(
                self.source_name,
                external_id,
                found,
                ats_type=detect_ats(found),
            ):
                updated += 1
        if updated:
            logger.info(
                f"remoteyeah updated {updated} stored jobs with off-site Apply URLs"
            )

    def _emit_jobs(
        self,
        page_jobs: list[dict[str, Any]],
        kept: list[dict[str, Any]],
        apply_session: "_ApplySession | None" = None,
    ) -> None:
        if not page_jobs:
            return
        unseen = set(
            unseen_listing_urls(
                [job["listing_url"] for job in page_jobs], self.source_name
            )
        )
        pending = [job for job in page_jobs if job["listing_url"] in unseen]
        if not pending:
            logger.info(
                f"remoteyeah {len(page_jobs)} engineering listings already seen — "
                "skipping apply capture"
            )
            return
        profile = load_candidate_profile()
        skipped = 0
        remembered: list[str] = []
        apply_session = apply_session or _ApplySession()
        for job in pending:
            if profile and exclusion_reason(_inclusion_fields(job), profile):
                skipped += 1
                remembered.append(job["listing_url"])
                continue
            apply_url = apply_session.resolve(job)
            if apply_url:
                job["url"] = apply_url
            self._emit(job, kept)
            remembered.append(job["listing_url"])
        if skipped:
            logger.info(
                f"remoteyeah skipped {skipped} ineligible listings before persist"
            )
        remember_listing_urls(self.source_name, remembered)

    def normalize(self, raw_job: dict[str, Any]) -> dict[str, Any]:
        location = raw_job.get("location") or "Remote"
        if not isinstance(location, str):
            location = "Remote"
        url = raw_job.get("url") or raw_job.get("listing_url") or ""
        description = raw_job.get("description", "")
        return {
            "external_id": str(raw_job.get("id") or url),
            "source": self.source_name,
            "company": (raw_job.get("company") or "").strip() or "Unknown",
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


def _is_engineering_title(title: str) -> bool:
    return any(kw in title.lower() for kw in _ENGINEERING_KEYWORDS)


def _plain(value: str) -> str:
    text = html_lib.unescape(_TAG_RE.sub(" ", value or ""))
    return _WS_RE.sub(" ", text).strip()


def _inner_tag(block: str, name: str) -> str:
    match = re.search(rf"<{name}[^>]*>(.*?)</{name}>", block, re.I | re.S)
    if not match:
        return ""
    raw = match.group(1) or ""
    cdata = _CDATA_RE.search(raw)
    if cdata:
        return cdata.group(1).strip()
    return raw.strip()


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = dateutil_parser.parse(value.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _strip_tracking(url: str) -> str:
    parts = urlsplit(url)
    query = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in {"ref", "source"}
    ]
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), "")
    )


def _clean_title(title: str, company: str) -> str:
    text = _plain(title)
    if company and text.lower().endswith(" at " + company.lower()):
        text = text[: -(len(company) + 4)].rstrip()
    if text.lower().startswith("remote "):
        text = text[7:].lstrip()
    return text.strip(" -–—") or _plain(title)


def _location_from_desc(desc: str) -> str:
    match = _LOC_RE.search(desc or "")
    if match:
        return _plain(match.group(1)) or "Remote"
    return "Remote"


def _inclusion_fields(job: dict[str, Any]) -> dict[str, Any]:
    location = job.get("location") or "Remote"
    return {
        "title": job.get("title") or "",
        "location": location,
        "raw_location_text": location,
        "description": job.get("description") or "",
        "description_text": job.get("description") or "",
    }


def _parse_item(block: str) -> dict[str, Any] | None:
    title_raw = _inner_tag(block, "title")
    link = _strip_tracking(html_lib.unescape(_inner_tag(block, "link")))
    if not title_raw or not link or "/jobs/" not in link:
        return None
    company = _plain(_inner_tag(block, "company")) or "Unknown"
    title = _clean_title(title_raw, company)
    if not title:
        return None
    desc = _inner_tag(block, "description")
    desc = html_lib.unescape(desc)
    posted = _parse_dt(_inner_tag(block, "pubDate"))
    job_id = link.rstrip("/").split("/")[-1] or title[:80]
    return {
        "id": job_id,
        "listing_url": link,
        "url": link,
        "title": title,
        "company": company,
        "location": _location_from_desc(desc),
        "description": desc,
        "posted_date": posted,
        "employment_type": _plain(_EMP_RE.search(desc).group(1))
        if _EMP_RE.search(desc)
        else "",
    }


def _fetch_feed() -> str | None:
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(
                FEED_URL,
                headers=_HEADERS,
                timeout=_FEED_TIMEOUT,
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"remoteyeah GET failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After") or "60"
            logger.info(f"remoteyeah HTTP 429 Retry-After={retry_after}")
            return None
        if resp.status_code >= 400:
            logger.info(f"remoteyeah HTTP {resp.status_code}")
            return None
        return resp.text or ""
    return None
