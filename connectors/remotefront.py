"""
RemoteFront connector.

Guest listing at
https://www.remotefront.com/remote-jobs?page=1&job_function=eng

Vercel Security Checkpoint blocks ``requests`` / ``curl_cffi``. Playwright
+ installed Chrome is the default fetch (same pattern as Ladders / Levels).
Listing ``goto`` soft-retries; a checkpoint or timeout logs INFO and keeps
jobs already emitted.

``job_function=eng`` is the pasted engineering filter. Sort order is not
proven newest-first — walk ``page=`` (runaway guard), date-filter, skip
known listing URLs via ``unseen_listing_urls``. Do not first-stale-stop or
prefix-slice. Engineering title filter. ``location`` is a string.

Detail page supplies description and an employer apply URL when present
(aggregator host → ``_LISTING_DOMAINS``).
"""
from __future__ import annotations

import html as html_lib
import json
import re
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_inclusion import exclusion_reason, load_candidate_profile
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

logger = setup_logger("remotefront_connector")

BASE_URL = "https://www.remotefront.com"
LISTING_PATH = "/remote-jobs"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
_NAV_TIMEOUT_MS = 60_000
_DETAIL_TIMEOUT_MS = 45_000
_OPEN_RETRIES = 3
_FETCH_DELAY = 0.4
# Mixed-date pager; runaway only.
_MAX_PAGES = 40
_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}
_JOB_HREF_RE = re.compile(
    r"""href=["']((?:https://(?:www\.)?remotefront\.com)?/remote-jobs/"""
    r"""([a-z0-9][a-z0-9-]{2,200}))["']""",
    re.I,
)
_RELATIVE_RE = re.compile(
    r"(?P<just>just now|today)|"
    r"(?:an?\s+(?P<one>minute|hour|day|week|month|year)\s+ago)|"
    r"(?P<n>\d+)\s+(?P<unit>minutes?|hours?|days?|weeks?|months?|years?)\s+ago",
    re.I,
)
_LD_JSON_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]+>")
_CHECKPOINT_MARKERS = (
    "vercel security checkpoint",
    "we're verifying your browser",
    "just a moment",
)
_EXTRACT_CARDS_JS = """() => {
  const text = (el) => (el && (el.innerText || el.textContent) || '')
    .replace(/\\s+/g, ' ').trim();
  const links = Array.from(
    document.querySelectorAll('a[href*="/remote-jobs/"]')
  );
  const seen = new Set();
  const out = [];
  for (const link of links) {
    const href = link.getAttribute('href') || '';
    const m = href.match(/\\/remote-jobs\\/([a-z0-9][a-z0-9-]{2,200})/i);
    if (!m) continue;
    const slug = m[1].toLowerCase();
    if (
      slug === 'search' ||
      slug.startsWith('category') ||
      seen.has(slug)
    ) continue;
    // Skip category / SEO hub pages that are not individual jobs.
    if (!/-([a-z0-9]{4,8})$/i.test(slug) && slug.split('-').length < 3) {
      continue;
    }
    seen.add(slug);
    const card = link.closest('article, li, [class*="card"], [class*="job"]')
      || link.parentElement
      || link;
    const blob = text(card);
    out.push({
      slug,
      href,
      title: text(link) || '',
      card_text: blob.slice(0, 500),
    });
  }
  return out;
}"""


class RemoteFrontConnector(BaseConnector):
    def __init__(self):
        self.source_name = "remotefront"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            "Fetching jobs from remotefront.com /remote-jobs "
            f"(job_function=eng, age_days={age_days}; mixed dates, "
            "no first-stale stop)…"
        )
        kept: list[dict[str, Any]] = []
        try:
            with _browser_session() as page:
                self._fetch_with_page(page, kept, cutoff)
        except Exception as e:
            logger.info(
                f"remotefront fetch aborted ({type(e).__name__}) — "
                f"keeping {len(kept)} prior jobs"
            )
        logger.info(f"Successfully fetched {len(kept)} jobs from remotefront")
        return kept

    def _fetch_with_page(
        self,
        page: Any,
        kept: list[dict[str, Any]],
        cutoff: datetime,
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        listed: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for page_num in range(1, _MAX_PAGES + 1):
            html = _open_listing(page, listing_url(page_num))
            if not html:
                logger.info(
                    f"remotefront page {page_num} skipped — "
                    f"keeping {len(listed)} listing cards so far"
                )
                break
            cards = _extract_cards_from_page(page, html, now=now)
            if not cards:
                logger.info(f"remotefront page {page_num}: 0 job cards")
                break
            page_jobs: list[dict[str, Any]] = []
            stale = 0
            for raw in cards:
                if raw["id"] in seen_ids:
                    continue
                seen_ids.add(raw["id"])
                posted = raw.get("posted_date")
                if posted and posted < cutoff:
                    stale += 1
                    continue
                if not _is_engineering_title(raw["title"]):
                    continue
                page_jobs.append(raw)
            listed.extend(page_jobs)
            logger.info(
                f"remotefront page {page_num}: {len(cards)} cards, "
                f"{len(page_jobs)} engineering in-window "
                f"({stale} stale skipped)"
            )
            if page_num < _MAX_PAGES:
                time.sleep(_FETCH_DELAY)
        self._hydrate(page, listed, kept, cutoff)

    def _hydrate(
        self,
        page: Any,
        listed: list[dict[str, Any]],
        kept: list[dict[str, Any]],
        cutoff: datetime,
    ) -> None:
        if not listed:
            return
        urls = [job["listing_url"] for job in listed]
        to_fetch = unseen_listing_urls(urls, self.source_name)
        wanted = set(to_fetch)
        pending = [job for job in listed if job["listing_url"] in wanted]
        if not pending:
            return
        profile = load_candidate_profile()
        skipped = 0
        remembered: list[str] = []
        for i, job in enumerate(pending):
            if profile and exclusion_reason(_inclusion_fields(job), profile):
                skipped += 1
                remembered.append(job["listing_url"])
                continue
            detail_html = _open_detail(page, job["listing_url"])
            if detail_html:
                if not _merge_detail(job, detail_html, cutoff):
                    skipped += 1
                    remembered.append(job["listing_url"])
                    continue
            self._emit(job, kept)
            remembered.append(job["listing_url"])
            if i + 1 < len(pending):
                time.sleep(_FETCH_DELAY)
        if skipped:
            logger.info(
                f"remotefront skipped {skipped} ineligible listings before persist"
            )
        if remembered:
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


def listing_url(page: int = 1) -> str:
    params = {"page": page, "job_function": "eng"}
    return f"{BASE_URL}{LISTING_PATH}?{urlencode(params)}"


def _is_engineering_title(title: str) -> bool:
    blob = f" {title.lower()} "
    return any(kw in blob for kw in _ENGINEERING_KEYWORDS)


def _inclusion_fields(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": job.get("title") or "",
        "location": job.get("location") or "Remote",
        "raw_location_text": job.get("location") or "Remote",
        "description": job.get("description") or "",
        "description_text": clean_description(job.get("description") or ""),
        "url": job.get("url") or job.get("listing_url") or "",
        "source": "remotefront",
    }


def _job_id_from_slug(slug: str) -> str:
    slug = (slug or "").strip().strip("/")
    if not slug:
        return ""
    # Trailing short token is the board id (e.g. v2pda, ume5g).
    parts = slug.split("-")
    if len(parts) >= 2 and 4 <= len(parts[-1]) <= 8 and parts[-1].isalnum():
        return parts[-1].lower()
    return slug.lower()


def _absolute_job_url(href: str, slug: str = "") -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href.split("?")[0].split("#")[0]
    if href.startswith("/"):
        return urljoin(BASE_URL, href.split("?")[0])
    if slug:
        return f"{BASE_URL}/remote-jobs/{slug}"
    return ""


def _parse_relative_date(
    text: str, now: datetime | None = None
) -> datetime | None:
    match = _RELATIVE_RE.search(text or "")
    if not match:
        return None
    now = now or datetime.now(tz=timezone.utc)
    if match.group("just"):
        return now
    if match.group("one"):
        unit = match.group("one").lower()
        amount = 1
    else:
        amount = int(match.group("n") or 0)
        unit = (match.group("unit") or "").lower()
    if unit.startswith("minute"):
        return now - timedelta(minutes=amount)
    if unit.startswith("hour"):
        return now - timedelta(hours=amount)
    if unit.startswith("day"):
        return now - timedelta(days=amount)
    if unit.startswith("week"):
        return now - timedelta(weeks=amount)
    if unit.startswith("month"):
        return now - timedelta(days=30 * amount)
    if unit.startswith("year"):
        return now - timedelta(days=365 * amount)
    return None


def _guess_company_title(
    slug: str, link_text: str, card_text: str
) -> tuple[str, str, str]:
    title = (link_text or "").strip()
    company = "Unknown"
    # "Title at Company" / "Company: Title"
    if " at " in title:
        left, _, right = title.rpartition(" at ")
        if left and right:
            title, company = left.strip(), right.strip()
    elif ": " in title:
        left, _, right = title.partition(": ")
        if left and right:
            company, title = left.strip(), right.strip()
    if company == "Unknown" and slug:
        # slug: company-title-words-id → company is first token when multi-part
        parts = slug.split("-")
        if len(parts) >= 3:
            company = parts[0].replace("_", " ").title()
    if not title:
        # Fall back to slug without trailing id.
        parts = slug.split("-")
        if len(parts) >= 2 and 4 <= len(parts[-1]) <= 8 and parts[-1].isalnum():
            parts = parts[:-1]
        if parts and parts[0].lower() == company.split()[0].lower():
            parts = parts[1:]
        title = " ".join(p.title() for p in parts) if parts else slug
    location = "Remote"
    loc_match = re.search(
        r"\b(Remote(?:\s*\([^)]+\))?|Worldwide|United States|US(?:A)?)\b",
        card_text or "",
        re.I,
    )
    if loc_match:
        location = loc_match.group(1)
    return title, company, location


def _parse_card(
    *,
    slug: str,
    href: str,
    title_hint: str,
    card_text: str,
    now: datetime,
) -> dict[str, Any] | None:
    slug = (slug or "").strip().lower()
    if not slug:
        return None
    job_id = _job_id_from_slug(slug)
    listing = _absolute_job_url(href, slug)
    if not listing:
        return None
    title, company, location = _guess_company_title(slug, title_hint, card_text)
    if not title:
        return None
    posted = _parse_relative_date(card_text, now=now)
    return {
        "id": job_id,
        "slug": slug,
        "title": title,
        "company": company,
        "location": location,
        "listing_url": listing,
        "url": listing,
        "description": "",
        "posted_date": posted,
    }


def _extract_cards(html: str, now: datetime | None = None) -> list[dict[str, Any]]:
    """Parse job cards from listing HTML (unit-testable, no Playwright)."""
    now = now or datetime.now(tz=timezone.utc)
    if _is_checkpoint(html):
        return []
    seen: set[str] = set()
    jobs: list[dict[str, Any]] = []
    for match in _JOB_HREF_RE.finditer(html or ""):
        href, slug = match.group(1), match.group(2).lower()
        if slug in seen:
            continue
        if not _looks_like_job_slug(slug):
            continue
        seen.add(slug)
        # Rough window of surrounding markup for title / date text.
        start = max(0, match.start() - 200)
        end = min(len(html), match.end() + 400)
        window = html[start:end]
        link_inner = ""
        # Prefer text of this anchor when present in a short form.
        close = re.search(
            rf"""href=["'][^"']*{re.escape(slug)}[^"']*["'][^>]*>(.*?)</a>""",
            window,
            re.I | re.DOTALL,
        )
        if close:
            link_inner = _TAG_RE.sub(" ", close.group(1))
            link_inner = html_lib.unescape(re.sub(r"\s+", " ", link_inner)).strip()
        card_text = _TAG_RE.sub(" ", window)
        card_text = html_lib.unescape(re.sub(r"\s+", " ", card_text)).strip()
        job = _parse_card(
            slug=slug,
            href=href,
            title_hint=link_inner,
            card_text=card_text,
            now=now,
        )
        if job:
            jobs.append(job)
    return jobs


def _looks_like_job_slug(slug: str) -> bool:
    slug = (slug or "").lower()
    if not slug or slug in {"search", "remote", "eng"}:
        return False
    parts = slug.split("-")
    if len(parts) < 3:
        return False
    # Prefer slug-id form used on the live board.
    if 4 <= len(parts[-1]) <= 8 and parts[-1].isalnum():
        return True
    return len(parts) >= 4


def _extract_cards_from_page(
    page: Any, html: str, now: datetime
) -> list[dict[str, Any]]:
    try:
        rows = page.evaluate(_EXTRACT_CARDS_JS)
    except Exception:
        rows = None
    if isinstance(rows, list) and rows:
        jobs: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            slug = str(row.get("slug") or "").lower()
            if not slug or slug in seen or not _looks_like_job_slug(slug):
                continue
            seen.add(slug)
            job = _parse_card(
                slug=slug,
                href=str(row.get("href") or ""),
                title_hint=str(row.get("title") or ""),
                card_text=str(row.get("card_text") or ""),
                now=now,
            )
            if job:
                jobs.append(job)
        if jobs:
            return jobs
    return _extract_cards(html, now=now)


def _is_checkpoint(html: str) -> bool:
    blob = (html or "").lower()
    return any(marker in blob for marker in _CHECKPOINT_MARKERS)


def _as_job_posting(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        type_ = data.get("@type")
        types = type_ if isinstance(type_, list) else [type_]
        if any(t == "JobPosting" for t in types):
            return data
        graph = data.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                found = _as_job_posting(item)
                if found:
                    return found
    if isinstance(data, list):
        for item in data:
            found = _as_job_posting(item)
            if found:
                return found
    return {}


def _job_posting(html: str) -> dict[str, Any]:
    for match in _LD_JSON_RE.finditer(html or ""):
        raw = match.group(1).strip()
        data: Any = None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            try:
                data = json.loads(html_lib.unescape(raw))
            except json.JSONDecodeError:
                continue
        posting = _as_job_posting(data)
        if posting:
            return posting
    return {}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        from dateutil import parser as dateutil_parser

        dt = dateutil_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _external_apply_url(html: str, listing_url: str) -> str:
    listing_host = urlparse(listing_url).netloc.lower().removeprefix("www.")
    # Prefer explicit apply anchors, then any external http(s) career link.
    candidates: list[str] = []
    for match in re.finditer(
        r"""<a[^>]+href=["'](https?://[^"']+)["'][^>]*>(.*?)</a>""",
        html or "",
        re.I | re.DOTALL,
    ):
        href = html_lib.unescape(match.group(1)).strip()
        label = _TAG_RE.sub(" ", match.group(2)).lower()
        host = urlparse(href).netloc.lower().removeprefix("www.")
        if not host or host == listing_host or "remotefront.com" in host:
            continue
        if any(
            p in label
            for p in ("apply", "company site", "careers", "original")
        ):
            return href.split("?")[0]
        candidates.append(href)
    return candidates[0].split("?")[0] if candidates else ""


def _merge_detail(job: dict[str, Any], html: str, cutoff: datetime) -> bool:
    if _is_checkpoint(html):
        return True  # keep listing fields
    detail = _job_posting(html)
    if detail:
        valid = _parse_dt(detail.get("validThrough"))
        if valid and valid < datetime.now(tz=timezone.utc):
            return False
        posted = _parse_dt(detail.get("datePosted"))
        if posted:
            if posted < cutoff:
                return False
            job["posted_date"] = posted
        title = (detail.get("title") or "").strip()
        if title:
            job["title"] = title
        org = detail.get("hiringOrganization")
        if isinstance(org, dict):
            name = (org.get("name") or "").strip()
            if name:
                job["company"] = name
        description = (detail.get("description") or "").strip()
        if description:
            job["description"] = description
        loc = detail.get("jobLocation")
        if isinstance(loc, dict):
            addr = loc.get("address") if isinstance(loc.get("address"), dict) else loc
            if isinstance(addr, dict):
                parts = [
                    str(addr.get(k) or "").strip()
                    for k in ("addressLocality", "addressRegion", "addressCountry")
                ]
                parts = [p for p in parts if p]
                if parts:
                    job["location"] = ", ".join(parts)
        elif isinstance(loc, str) and loc.strip():
            job["location"] = loc.strip()
    if not job.get("description"):
        # Fallback: strip main text blob.
        text = _TAG_RE.sub(" ", html or "")
        text = html_lib.unescape(re.sub(r"\s+", " ", text)).strip()
        if len(text) > 200:
            job["description"] = text[:8000]
    apply_url = _external_apply_url(html, job.get("listing_url") or "")
    if apply_url:
        job["url"] = apply_url
    return True


def _launch_browser(pw: Any) -> Any:
    args = ["--disable-http2", "--disable-blink-features=AutomationControlled"]
    try:
        return pw.chromium.launch(headless=True, channel="chrome", args=args)
    except Exception:
        return pw.chromium.launch(headless=True, args=args)


@contextmanager
def _browser_session():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = _launch_browser(pw)
        context = None
        try:
            context = browser.new_context(user_agent=_UA, locale="en-US")
            page = context.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            yield page
        finally:
            if context is not None:
                context.close()
            browser.close()


def _open_listing(page: Any, url: str) -> str:
    last_err: Exception | None = None
    for attempt in range(1, _OPEN_RETRIES + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
            try:
                page.wait_for_selector(
                    'a[href*="/remote-jobs/"]', timeout=15_000
                )
            except Exception:
                pass
            html = page.content() or ""
            if _is_checkpoint(html):
                logger.info(
                    f"remotefront checkpoint on listing "
                    f"attempt {attempt}/{_OPEN_RETRIES}"
                )
                last_err = RuntimeError("vercel checkpoint")
                if attempt < _OPEN_RETRIES:
                    page.wait_for_timeout(1500 * attempt)
                    continue
                return ""
            return html
        except Exception as e:
            last_err = e
            logger.info(
                f"remotefront listing failed ({type(e).__name__}) "
                f"attempt {attempt}/{_OPEN_RETRIES}"
            )
            if attempt < _OPEN_RETRIES:
                try:
                    page.wait_for_timeout(1000 * attempt)
                except Exception:
                    pass
                continue
    if last_err is not None:
        logger.info(
            f"remotefront listing skipped after {_OPEN_RETRIES} attempts "
            f"({type(last_err).__name__})"
        )
    return ""


def _open_detail(page: Any, url: str) -> str:
    last_err: Exception | None = None
    for attempt in range(1, _OPEN_RETRIES + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=_DETAIL_TIMEOUT_MS)
            page.wait_for_timeout(800)
            html = page.content() or ""
            if _is_checkpoint(html):
                logger.info(
                    f"remotefront checkpoint on detail "
                    f"attempt {attempt}/{_OPEN_RETRIES}"
                )
                last_err = RuntimeError("vercel checkpoint")
                if attempt < _OPEN_RETRIES:
                    page.wait_for_timeout(1500 * attempt)
                    continue
                return ""
            return html
        except Exception as e:
            last_err = e
            logger.info(
                f"remotefront detail failed ({type(e).__name__}) "
                f"attempt {attempt}/{_OPEN_RETRIES}"
            )
            if attempt < _OPEN_RETRIES:
                try:
                    page.wait_for_timeout(1000 * attempt)
                except Exception:
                    pass
                continue
    if last_err is not None:
        logger.info(
            f"remotefront detail skipped after {_OPEN_RETRIES} attempts "
            f"({type(last_err).__name__})"
        )
    return ""
