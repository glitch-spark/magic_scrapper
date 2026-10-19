"""
OmniJobs connector.

Guest search at
https://omnijobs.io/en/search?location=US&location=ANYWHERE_IN_LATAM
&location=ANYWHERE_IN_WORLD&location=ANYWHERE_IN_EUROPE
&locationType=remote&jobFunction=software+development

Vercel Security Checkpoint blocks ``requests`` / ``curl_cffi``. Playwright
+ installed Chrome is the default fetch (same pattern as RemoteFront).
Listing ``goto`` soft-retries; a checkpoint or timeout logs INFO and keeps
jobs already emitted.

``locationType=remote`` and ``jobFunction=software development`` match the
pasted URL, plus the four location tokens. Public cards show
``Opened … ago`` newest-first (indexed role page, 2026-09-22). When a page's
dates are descending, stop at the first stale card. Otherwise date-filter
and follow ``rel=next`` / ``page=`` only. The free list ends at an OmniJobs
Pro wall ("see every result, not just page one") — do not walk past it.

Engineering title filter. ``location`` is a string. Skip detail when the
card already has title, location, and description. Detail JobPosting /
``applicationUrl`` supplies the JD and an employer apply URL when present.
Full results and extra applications are Pro. Aggregator
host → ``_LISTING_DOMAINS``.
"""
from __future__ import annotations

import html as html_lib
import json
import re
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_inclusion import exclusion_reason, load_candidate_profile
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

logger = setup_logger("omnijobs_connector")

BASE_URL = "https://omnijobs.io"
LISTING_PATH = "/en/search"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
_NAV_TIMEOUT_MS = 45_000
_DETAIL_TIMEOUT_MS = 30_000
_OPEN_RETRIES = 3
_FETCH_DELAY = 0.4
# Runaway only. Newest-first pages stop at the first stale card.
_MAX_PAGES = 15
_LOCATIONS = (
    "US",
    "ANYWHERE_IN_LATAM",
    "ANYWHERE_IN_WORLD",
    "ANYWHERE_IN_EUROPE",
)
_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}
_ARTICLE_RE = re.compile(r"<article\b[^>]*>(.*?)</article>", re.I | re.S)
_HREF_RE = re.compile(
    r"""href=["']([^"']*?/en/jobs/(\d+))["'][^>]*>(.*?)</a>""",
    re.I | re.S,
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_OPENED_RE = re.compile(
    r"(?:opened\s+)?(?:(?P<n>\d+)\s*)?"
    r"(?P<unit>min(?:ute)?s?|h(?:our)?s?|d(?:ay)?s?|w(?:eek)?s?|mo(?:nth)?s?)"
    r"\s+ago",
    re.I,
)
_EMPLOYEES_RE = re.compile(
    r"([A-Z][\w&.'’,+-]*(?:\s+[A-Z][\w&.'’,+-]*){0,5})\s+\d+\s+employees",
)
_LD_JSON_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)
_APP_URL_RE = re.compile(
    r'"applicationUrl"\s*:\s*"(https?://[^"]+)"',
    re.I,
)
_A_TAG_RE = re.compile(r"<a\b([^>]*)>", re.I)
_HREF_ATTR_RE = re.compile(r"""href=["']([^"']+)["']""", re.I)
_REL_NEXT_RE = re.compile(r"""\brel=["'][^"']*\bnext\b[^"']*["']""", re.I)
_CHECKPOINT_MARKERS = (
    "vercel security checkpoint",
    "we're verifying your browser",
    "just a moment",
)
_PRO_MARKERS = (
    "omnijobs pro",
    "not just page one",
    "unlocking all job posts",
)
_US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming",
}


class OmniJobsConnector(BaseConnector):
    def __init__(self):
        self.source_name = "omnijobs"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            "Fetching jobs from omnijobs.io /en/search "
            f"(remote, software development, age_days={age_days}; "
            "newest-first first-stale when Opened dates descend)…"
        )
        kept: list[dict[str, Any]] = []
        try:
            with _browser_session() as page:
                self._fetch_with_page(page, kept, cutoff)
        except Exception as e:
            logger.info(
                f"omnijobs fetch aborted ({type(e).__name__}) — "
                f"keeping {len(kept)} prior jobs"
            )
        logger.info(f"Successfully fetched {len(kept)} jobs from omnijobs")
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
        url = listing_url(1)
        for page_num in range(1, _MAX_PAGES + 1):
            html = _open_listing(page, url)
            if not html:
                logger.info(
                    f"omnijobs page {page_num} skipped — "
                    f"keeping {len(listed)} listing cards so far"
                )
                break
            cards = _extract_cards(html, now=now)
            if not cards:
                logger.info(f"omnijobs page {page_num}: 0 job cards")
                break
            newest = _page_is_newest_first(cards)
            page_jobs: list[dict[str, Any]] = []
            stale_stop = False
            skipped_stale = 0
            for raw in cards:
                if raw["id"] in seen_ids:
                    continue
                seen_ids.add(raw["id"])
                posted = raw.get("posted_date")
                if posted and posted < cutoff:
                    if newest:
                        logger.info(
                            "omnijobs first stale Opened date — "
                            "stopping newest-first walk"
                        )
                        stale_stop = True
                        break
                    skipped_stale += 1
                    continue
                if not _is_engineering_title(raw["title"]):
                    continue
                page_jobs.append(raw)
            listed.extend(page_jobs)
            logger.info(
                f"omnijobs page {page_num}: {len(cards)} cards, "
                f"{len(page_jobs)} engineering in-window "
                f"({skipped_stale} stale skipped)"
                f"{' (newest-first)' if newest else ''}"
            )
            if stale_stop or _is_pro_wall(html):
                if _is_pro_wall(html) and not stale_stop:
                    logger.info(
                        "omnijobs free preview ends at OmniJobs Pro — "
                        "not walking further pages"
                    )
                break
            nxt = _next_listing_url(html, url)
            if not nxt or nxt == url:
                break
            url = nxt
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
        unseen = unseen_listing_urls(
            [job["listing_url"] for job in listed],
            self.source_name,
        )
        pending_urls = set(unseen)
        pending = [job for job in listed if job["listing_url"] in pending_urls]
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
            if not _listing_is_complete(job):
                html = _open_detail(page, job["listing_url"])
                if html and not _merge_detail(job, html, cutoff):
                    skipped += 1
                    remembered.append(job["listing_url"])
                    continue
                if profile and exclusion_reason(_inclusion_fields(job), profile):
                    skipped += 1
                    remembered.append(job["listing_url"])
                    continue
            self._emit(job, kept)
            remembered.append(job["listing_url"])
            if i + 1 < len(pending):
                time.sleep(_FETCH_DELAY)
        if skipped:
            logger.info(
                f"omnijobs skipped {skipped} ineligible listings before persist"
            )
        if remembered:
            remember_listing_urls(self.source_name, remembered)

    def normalize(self, raw_job: dict[str, Any]) -> dict[str, Any]:
        location = raw_job.get("location") or "Remote"
        if not isinstance(location, str):
            location = "Remote"
        url = raw_job.get("url") or raw_job.get("listing_url") or ""
        description = raw_job.get("description") or ""
        return {
            "external_id": str(raw_job.get("id") or url),
            "source": self.source_name,
            "company": (raw_job.get("company") or "").strip() or "Unknown",
            "title": raw_job.get("title") or "",
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
    params: list[tuple[str, str]] = [
        ("location", loc) for loc in _LOCATIONS
    ]
    params.append(("locationType", "remote"))
    params.append(("jobFunction", "software development"))
    if page > 1:
        params.append(("page", str(page)))
    return f"{BASE_URL}{LISTING_PATH}?{urlencode(params)}"


def _is_engineering_title(title: str) -> bool:
    blob = f" {title.lower().replace('-', ' ')} "
    return any(kw in blob for kw in _ENGINEERING_KEYWORDS)


def _plain(value: str) -> str:
    text = html_lib.unescape(_TAG_RE.sub(" ", value or ""))
    return _WS_RE.sub(" ", text).strip()


def _is_checkpoint(html: str) -> bool:
    low = (html or "").lower()
    return any(marker in low for marker in _CHECKPOINT_MARKERS)


def _is_pro_wall(html: str) -> bool:
    low = (html or "").lower()
    return any(marker in low for marker in _PRO_MARKERS)


def _parse_opened(text: str, now: datetime) -> datetime | None:
    match = _OPENED_RE.search(text or "")
    if not match:
        return None
    n = int(match.group("n") or "1")
    unit = (match.group("unit") or "").lower()
    if unit.startswith("min"):
        delta = timedelta(minutes=n)
    elif unit.startswith("h"):
        delta = timedelta(hours=n)
    elif unit.startswith("d"):
        delta = timedelta(days=n)
    elif unit.startswith("w"):
        delta = timedelta(weeks=n)
    else:
        delta = timedelta(days=30 * n)
    return now - delta


def _city_state(text: str) -> str:
    """Return 'City, State' when a US state name closes the place line."""
    low = (text or "").lower()
    for state in sorted(_US_STATES, key=len, reverse=True):
        idx = low.rfind(state)
        if idx <= 0:
            continue
        before = text[:idx].strip(" ,|-")
        city_words: list[str] = []
        for word in reversed(before.split()):
            low = word.lower().strip(",.")
            if low in {"ago", "opened", "remote", "job", "actions", "repost"}:
                break
            if any(ch.isdigit() for ch in word):
                break
            city_words.append(word)
            if len(city_words) == 3:
                break
        if not city_words:
            continue
        city = " ".join(reversed(city_words))
        return f"{city}, {state.title()}"
    return ""


def _location_text(text: str) -> str:
    blob = f" {(text or '').lower()} "
    place = _city_state(text or "")
    worldwide = (
        "remote remote" in blob
        or "worldwide" in blob
        or "anywhere in the world" in blob
        or "anywhere_in_world" in blob
    )
    us = bool(
        re.search(r"\bremote\s+(usa|u\.s\.|us)\b", blob)
        or "united states" in blob
        or "🇺🇸" in (text or "")
    )
    if worldwide and not place:
        return "Remote"
    if us and not place:
        return "Remote (US)"
    if "latam" in blob or "latin america" in blob:
        return f"Remote, {place}" if place else "Remote (LATAM)"
    if re.search(r"\beurope\b", blob) and "european" not in blob:
        return f"Remote, {place}" if place else "Remote (Europe)"
    remote = "remote" in blob
    if place and remote:
        return f"Remote, {place}"
    if place:
        return place
    if remote:
        return "Remote"
    return "Remote"


def _company_from_text(text: str) -> str:
    matches = list(_EMPLOYEES_RE.finditer(text or ""))
    if not matches:
        return "Unknown"
    parts = matches[-1].group(1).strip(" -|").split()
    drop = {"usa", "us", "remote", "europe", "latam", "opened"}
    while parts and parts[0].lower().strip(",.") in drop:
        parts = parts[1:]
    name = " ".join(parts)
    return name or "Unknown"


def _card_chunks(html: str) -> list[str]:
    articles = _ARTICLE_RE.findall(html or "")
    if articles:
        return articles
    parts = re.split(r'(?=<a\b[^>]*href=["\'][^"\']*?/en/jobs/\d+)', html or "", flags=re.I)
    return [part for part in parts if "/en/jobs/" in part]


def _extract_cards(html: str, *, now: datetime) -> list[dict[str, Any]]:
    if not html or _is_checkpoint(html):
        return []
    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for chunk in _card_chunks(html):
        match = _HREF_RE.search(chunk)
        if not match:
            continue
        href, job_id, anchor = match.group(1), match.group(2), match.group(3)
        if job_id in seen:
            continue
        seen.add(job_id)
        text = _plain(chunk)
        title = _plain(anchor)
        if len(title) < 3 or title.lower() in {"job actions", "remote"}:
            title = _title_from_text(text)
        if not title:
            continue
        listing = urljoin(BASE_URL, href)
        jobs.append({
            "id": job_id,
            "listing_url": listing,
            "url": listing,
            "title": title,
            "company": _company_from_text(text),
            "location": _location_text(text),
            "description": "",
            "posted_date": _parse_opened(text, now),
        })
    return jobs


def _title_from_text(text: str) -> str:
    for line in re.split(r"\s{2,}|(?<=\.) ", text or ""):
        line = line.strip()
        low = line.lower()
        if len(line) < 4:
            continue
        if low.startswith("opened") or low.startswith("remote"):
            continue
        if "employees" in low or low.startswith("http"):
            continue
        return line[:180]
    return ""


def _page_is_newest_first(jobs: list[dict[str, Any]]) -> bool:
    dates = [job["posted_date"] for job in jobs if job.get("posted_date")]
    if len(dates) < 2:
        return False
    return all(dates[i] >= dates[i + 1] for i in range(len(dates) - 1))


def _next_listing_url(html: str, current: str) -> str | None:
    if _is_pro_wall(html):
        return None
    current_page = _page_num(current)
    found: list[str] = []
    for attrs in _A_TAG_RE.findall(html or ""):
        href_m = _HREF_ATTR_RE.search(attrs)
        if not href_m:
            continue
        href = html_lib.unescape(href_m.group(1))
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(BASE_URL, href)
        if "omnijobs.io" not in abs_url:
            continue
        if "/en/jobs/" in abs_url:
            continue
        is_next = bool(_REL_NEXT_RE.search(attrs))
        page_n = _page_num(abs_url)
        if is_next or (page_n and page_n == current_page + 1):
            found.append(abs_url)
    return found[0] if found else None


def _page_num(url: str) -> int:
    try:
        query = dict(parse_qsl(urlparse(url).query))
    except Exception:
        return 1
    try:
        return int(query.get("page") or "1")
    except ValueError:
        return 1


def _listing_is_complete(job: dict[str, Any]) -> bool:
    return bool(
        (job.get("title") or "").strip()
        and (job.get("location") or "").strip()
        and (job.get("description") or "").strip()
    )


def _inclusion_fields(job: dict[str, Any]) -> dict[str, Any]:
    location = job.get("location") or "Remote"
    description = job.get("description") or ""
    return {
        "title": job.get("title") or "",
        "location": location,
        "raw_location_text": location,
        "description": description,
        "description_text": description,
        "url": job.get("url") or job.get("listing_url") or "",
        "source": "omnijobs",
    }


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


def _external_apply_url(html: str) -> str:
    match = _APP_URL_RE.search(html or "")
    if match:
        url = match.group(1)
        if "omnijobs.io" not in url:
            return url
    posting = _job_posting(html)
    for key in ("url", "applicationUrl"):
        value = posting.get(key)
        if isinstance(value, str) and value.startswith("http") and "omnijobs.io" not in value:
            return value
    return ""


def _merge_detail(job: dict[str, Any], html: str, cutoff: datetime) -> bool:
    if _is_checkpoint(html):
        return True
    detail = _job_posting(html)
    if detail:
        posted = detail.get("datePosted")
        if posted:
            try:
                from dateutil import parser as dateutil_parser

                dt = dateutil_parser.parse(str(posted))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone(timezone.utc)
                if dt < cutoff:
                    return False
                job["posted_date"] = dt
            except Exception:
                pass
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
                    prefix = "Remote, " if "remote" in (job.get("location") or "").lower() else ""
                    job["location"] = prefix + ", ".join(parts)
        elif isinstance(loc, str) and loc.strip() and not loc.strip().startswith("{"):
            job["location"] = loc.strip()
    apply_url = _external_apply_url(html)
    if apply_url:
        job["url"] = apply_url
    if not job.get("description"):
        text = _plain(html)
        if len(text) > 200:
            job["description"] = text[:8000]
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
            page.wait_for_timeout(1500)
            html = page.content() or ""
            if _is_checkpoint(html):
                logger.info(
                    f"omnijobs checkpoint on listing "
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
                f"omnijobs listing failed ({type(e).__name__}) "
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
            f"omnijobs listing skipped after {_OPEN_RETRIES} attempts "
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
                    f"omnijobs checkpoint on detail "
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
                f"omnijobs detail failed ({type(e).__name__}) "
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
            f"omnijobs detail skipped after {_OPEN_RETRIES} attempts "
            f"({type(last_err).__name__})"
        )
    return ""
