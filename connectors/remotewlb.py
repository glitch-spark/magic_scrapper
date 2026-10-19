"""
RemoteWLB connector.

Guest sitemaps (``robots.txt`` Allow: /):
``GET https://remotewlb.com/sitemap.xml`` indexes monthly shards at
``/sitemaps/jobs/YYYY-MM-1``. Listing HTML at ``/jobs`` is a Next.js shell
with no job cards — unused. No public RSS/API.

Each month urlset is oldest-first by ``lastmod`` (live check 2026-09-22).
Reverse each shard so the walk is newest-first, then stop at the first
stale ``lastmod``. Walk newest month shards first (index ``lastmod``).
Engineering title filter (slug + JobPosting). Skip expired
``validThrough``. Detail JobPosting JSON-LD for description/company/
``datePosted``. ``location`` is ``Remote`` (TELECOMMUTE).

``directApply`` is false and there is no employer apply URL.
Fetch via Chrome TLS (``curl_cffi``) with soft retries; plain ``requests``
fallback. Job pages are fetched four at a time (10s timeout, one retry).
The sitemap index and month shard stay on a 60s timeout with 3 retries.
"""
from __future__ import annotations

import html as html_lib
import json
import re
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_inclusion import exclusion_reason, load_candidate_profile
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

logger = setup_logger("remotewlb_connector")

BASE_URL = "https://remotewlb.com"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
LISTING_URL = f"{BASE_URL}/jobs"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,text/html;q=0.9,*/*;q=0.8",
    "Referer": LISTING_URL,
}
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
_SITEMAP_TIMEOUT = 60
_SITEMAP_RETRIES = 3
_DETAIL_TIMEOUT = 10
_DETAIL_RETRIES = 2
_DETAIL_WORKERS = 4
_RETRY_DELAY = 1.5
_FETCH_DELAY = 0.4
# Runaway: how many month shards to open (newest first).
_MAX_MONTH_SHARDS = 6
_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}
_JOB_PATH_RE = re.compile(
    r"^https://(?:www\.)?remotewlb\.com/job/([a-z0-9][a-z0-9-]{2,200})$",
    re.I,
)
_JOB_SITEMAP_RE = re.compile(
    r"^https://(?:www\.)?remotewlb\.com/sitemaps/jobs/\d{4}-\d{2}-\d+$",
    re.I,
)
_LD_JSON_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

_CURL_VERIFY: bool | None = None


class RemoteWlbConnector(BaseConnector):
    def __init__(self):
        self.source_name = "remotewlb"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            "Fetching jobs from remotewlb.com job sitemaps "
            f"(newest-first lastmod after reverse, age_days={age_days}; "
            "stop at first stale job)…"
        )
        kept: list[dict[str, Any]] = []
        shards = _fetch_month_shard_urls()
        if not shards:
            logger.info("remotewlb sitemap index empty or unreachable")
            return []
        page_jobs: list[dict[str, Any]] = []
        stale_stop = False
        for shard_url in shards[:_MAX_MONTH_SHARDS]:
            entries = _fetch_month_entries(shard_url)
            if not entries:
                logger.info(
                    f"remotewlb shard skipped — keeping {len(page_jobs)} "
                    "listing cards so far"
                )
                continue
            for entry in entries:
                posted = entry.get("posted_date")
                if posted is not None and posted < cutoff:
                    logger.info(
                        "remotewlb first stale lastmod — "
                        "stopping newest-first walk"
                    )
                    stale_stop = True
                    break
                if not _is_engineering_title(entry["title"]):
                    continue
                page_jobs.append(entry)
            logger.info(
                f"remotewlb {shard_url.rsplit('/', 1)[-1]}: "
                f"{len(entries)} urls → {len(page_jobs)} engineering "
                f"in-window so far"
                f"{' (stale stop)' if stale_stop else ''}"
            )
            if stale_stop:
                break
            time.sleep(_FETCH_DELAY)
        self._emit_listings(page_jobs, kept, cutoff)
        logger.info(f"Successfully fetched {len(kept)} jobs from remotewlb")
        return kept

    def _emit_listings(
        self,
        page_jobs: list[dict[str, Any]],
        kept: list[dict[str, Any]],
        cutoff: datetime,
    ) -> None:
        if not page_jobs:
            return
        unseen = unseen_listing_urls(
            [job["listing_url"] for job in page_jobs],
            self.source_name,
        )
        pending_urls = set(unseen)
        pending = [job for job in page_jobs if job["listing_url"] in pending_urls]
        if not pending:
            return
        profile = load_candidate_profile()
        skipped = 0
        remembered: list[str] = []
        to_fetch: list[dict[str, Any]] = []
        for job in pending:
            if profile and exclusion_reason(_inclusion_fields(job), profile):
                skipped += 1
                remembered.append(job["listing_url"])
                continue
            to_fetch.append(job)

        lock = threading.Lock()

        def _one(job: dict[str, Any]) -> None:
            nonlocal skipped
            html = _fetch_bytes(
                job["listing_url"],
                "detail",
                timeout=_DETAIL_TIMEOUT,
                retries=_DETAIL_RETRIES,
            )
            with lock:
                if html:
                    if not _merge_detail(
                        job, html.decode("utf-8", "replace"), cutoff
                    ):
                        skipped += 1
                        remembered.append(job["listing_url"])
                        return
                    if profile and exclusion_reason(_inclusion_fields(job), profile):
                        skipped += 1
                        remembered.append(job["listing_url"])
                        return
                self._emit(job, kept)
                remembered.append(job["listing_url"])

        if to_fetch:
            workers = max(1, min(_DETAIL_WORKERS, len(to_fetch)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(_one, job) for job in to_fetch]
                for fut in as_completed(futures):
                    fut.result()
        if skipped:
            logger.info(
                f"remotewlb skipped {skipped} ineligible listings before persist"
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


def _is_engineering_title(title: str) -> bool:
    blob = f" {title.lower().replace('-', ' ')} "
    return any(kw in blob for kw in _ENGINEERING_KEYWORDS)


def _plain(value: str) -> str:
    text = html_lib.unescape(_TAG_RE.sub(" ", value or ""))
    return _WS_RE.sub(" ", text).strip()


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        dt = dateutil_parser.parse(str(value).strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _title_from_slug(slug: str) -> str:
    parts = (slug or "").split("-")
    # Drop trailing numeric board id when present.
    if len(parts) >= 2 and parts[-1].isdigit():
        parts = parts[:-1]
    return _plain(" ".join(parts)).title()


def _parse_job_url(url: str) -> dict[str, str] | None:
    match = _JOB_PATH_RE.match((url or "").strip())
    if not match:
        return None
    slug = match.group(1)
    parts = slug.split("-")
    job_id = parts[-1] if parts and parts[-1].isdigit() else slug
    title = _title_from_slug(slug)
    if not title:
        return None
    return {"id": job_id, "slug": slug, "title": title}


def _inclusion_fields(job: dict[str, Any]) -> dict[str, Any]:
    location = job.get("location") or "Remote"
    return {
        "title": job.get("title") or "",
        "location": location,
        "raw_location_text": location,
        "description": job.get("description") or "",
        "description_text": job.get("description") or "",
        "url": job.get("url") or job.get("listing_url") or "",
        "source": "remotewlb",
    }


def _parse_sitemap_index(content: bytes) -> list[tuple[str, datetime | None]]:
    """Return (shard_url, lastmod) for job month sitemaps, newest first."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []
    entries: list[tuple[str, datetime | None]] = []
    for sm in root.findall("sm:sitemap", _NS) or root.findall("sitemap"):
        loc = (
            sm.findtext("sm:loc", namespaces=_NS) or sm.findtext("loc") or ""
        ).strip()
        if not _JOB_SITEMAP_RE.match(loc):
            continue
        lastmod = (
            sm.findtext("sm:lastmod", namespaces=_NS)
            or sm.findtext("lastmod")
            or ""
        ).strip()
        entries.append((loc, _parse_dt(lastmod)))
    entries.sort(
        key=lambda row: row[1] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return entries


def _parse_month_urlset(content: bytes) -> list[dict[str, Any]]:
    """Parse a month urlset and return jobs newest-first (reverse oldest-first)."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []
    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for url_el in root.findall("sm:url", _NS) or root.findall("url"):
        loc = (
            url_el.findtext("sm:loc", namespaces=_NS)
            or url_el.findtext("loc")
            or ""
        ).strip()
        parsed = _parse_job_url(loc)
        if not parsed or loc in seen:
            continue
        seen.add(loc)
        lastmod = (
            url_el.findtext("sm:lastmod", namespaces=_NS)
            or url_el.findtext("lastmod")
            or ""
        ).strip()
        jobs.append({
            "id": parsed["id"],
            "listing_url": loc,
            "url": loc,
            "title": parsed["title"],
            "company": "Unknown",
            "location": "Remote",
            "description": "",
            "posted_date": _parse_dt(lastmod),
        })
    # Live month shards are oldest-first; reverse for newest-first walk.
    jobs.reverse()
    return jobs


def _fetch_bytes(
    url: str,
    label: str,
    *,
    timeout: int = _SITEMAP_TIMEOUT,
    retries: int = _SITEMAP_RETRIES,
) -> bytes | None:
    data = _fetch_via_curl_cffi(url, label, timeout=timeout, retries=retries)
    if data is not None:
        return data
    return _fetch_via_requests(url, label, timeout=timeout, retries=retries)


def _fetch_via_curl_cffi(
    url: str, label: str, *, timeout: int, retries: int
) -> bytes | None:
    global _CURL_VERIFY
    try:
        from curl_cffi import requests as chrome_requests
    except ImportError:
        return None

    def _get(*, verify: bool):
        return chrome_requests.get(
            url,
            impersonate="chrome",
            timeout=timeout,
            allow_redirects=True,
            verify=verify,
            headers=_HEADERS,
        )

    for attempt in range(1, retries + 1):
        verify = True if _CURL_VERIFY is None else _CURL_VERIFY
        try:
            resp = _get(verify=verify)
        except Exception as e:
            is_cert = (
                "certificate" in str(e).lower()
                or "ssl" in type(e).__name__.lower()
            )
            if is_cert and _CURL_VERIFY is not False:
                _CURL_VERIFY = False
                try:
                    resp = _get(verify=False)
                except Exception as e2:
                    logger.info(
                        f"remotewlb {label} chrome-TLS failed "
                        f"({type(e2).__name__}) attempt {attempt}/{retries}"
                    )
                    if attempt < retries:
                        time.sleep(_RETRY_DELAY * attempt)
                    continue
            else:
                logger.info(
                    f"remotewlb {label} chrome-TLS failed "
                    f"({type(e).__name__}) attempt {attempt}/{retries}"
                )
                if attempt < retries:
                    time.sleep(_RETRY_DELAY * attempt)
                continue
        else:
            if _CURL_VERIFY is None:
                _CURL_VERIFY = verify

        if resp.status_code >= 400:
            logger.info(
                f"remotewlb {label} chrome-TLS HTTP {resp.status_code} "
                f"attempt {attempt}/{retries}"
            )
            if attempt < retries:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.content or b""
    logger.info(f"remotewlb {label} chrome-TLS skipped after {retries} attempts")
    return None


def _fetch_via_requests(
    url: str, label: str, *, timeout: int, retries: int
) -> bytes | None:
    import requests

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"remotewlb {label} requests failed ({type(e).__name__}) "
                f"attempt {attempt}/{retries}"
            )
            if attempt < retries:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"remotewlb {label} requests HTTP {resp.status_code} "
                f"attempt {attempt}/{retries}"
            )
            if attempt < retries:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.content or b""
    logger.info(f"remotewlb {label} requests skipped after {retries} attempts")
    return None


def _fetch_month_shard_urls() -> list[str]:
    content = _fetch_bytes(
        SITEMAP_URL,
        "sitemap-index",
        timeout=_SITEMAP_TIMEOUT,
        retries=_SITEMAP_RETRIES,
    )
    if content is None:
        return []
    return [loc for loc, _ in _parse_sitemap_index(content)]


def _fetch_month_entries(shard_url: str) -> list[dict[str, Any]]:
    content = _fetch_bytes(
        shard_url,
        "month-sitemap",
        timeout=_SITEMAP_TIMEOUT,
        retries=_SITEMAP_RETRIES,
    )
    if content is None:
        return []
    return _parse_month_urlset(content)


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


def _merge_detail(job: dict[str, Any], html: str, cutoff: datetime) -> bool:
    detail = _job_posting(html)
    if not detail:
        return True
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
    # Board is remote-only; JSON-LD uses TELECOMMUTE without a place string.
    job["location"] = "Remote"
    # directApply is false and JobPosting has no employer url — keep listing URL.
    return True
