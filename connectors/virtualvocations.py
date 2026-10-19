"""
VirtualVocations connector.

Guest sitemap at GET https://www.virtualvocations.com/sitemap/index/sitemap.xml
(robots allows ``/`` and ``/job/*``). Live urlset is newest-first by
``lastmod`` (checked 2026-09-21, 0 inversions). Job URLs:
``/job/{slug}-{id}-i.html``.

The pasted search (``s-date``, ``d-48``) is newest-first too, but listing
HTML 429s without a ``vv_edge`` cookie and career-level path segments
(``c-experienced`` / ``c-senior+level`` / ``c-management``) are seniority —
that stays in ``job_inclusion``. Stacking IT + Engineering categories is
AND and returned no results.

Stop at the first stale ``lastmod``. Engineering title from the slug.
Skip known URLs. Skip detail when listing title + ``Remote`` fails
inclusion. Guest job HTML has a Job Summary; full JD / company / apply are
membership gated. ``location`` is a string.
"""
from __future__ import annotations

import html as html_lib
import re
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import requests
from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_inclusion import exclusion_reason, load_candidate_profile
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

logger = setup_logger("virtualvocations_connector")

BASE_URL = "https://www.virtualvocations.com"
SITEMAP_URL = f"{BASE_URL}/sitemap/index/sitemap.xml"
LISTING_URL = (
    f"{BASE_URL}/jobs/c-experienced/c-senior+level/c-management/d-48/s-date"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": f"{BASE_URL}/jobs/s-date",
}
_SITEMAP_HEADERS = {**_HEADERS, "Accept": "application/xml,text/xml,*/*"}
_COOKIES = {"vv_edge": "1"}
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
_SITEMAP_TIMEOUT = 40
_DETAIL_TIMEOUT = 25
_RETRIES = 3
_RETRY_DELAY = 1.5
_FETCH_DELAY = 0.4
_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}
_JOB_PATH_RE = re.compile(
    r"^https://www\.virtualvocations\.com/job/(.+)-(\d+)-i\.html$",
    re.I,
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
_LOCATION_RE = re.compile(
    r"Location:\s*(.+?)(?=\s*(?:Compensation:|Reviewed:|This job expires|<))",
    re.I | re.S,
)
_SUMMARY_RE = re.compile(
    r"<h2[^>]*>\s*Job Summary\s*</h2>(.*?)(?=<h2\b)",
    re.I | re.S,
)


class VirtualVocationsConnector(BaseConnector):
    def __init__(self):
        self.source_name = "virtualvocations"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            "Fetching jobs from virtualvocations.com sitemap "
            f"(newest-first lastmod, age_days={age_days}; "
            "stop at first stale job; no career-level path)…"
        )
        kept: list[dict[str, Any]] = []
        try:
            entries = _fetch_sitemap()
            if not entries:
                logger.info("virtualvocations sitemap empty")
                return []
            page_jobs: list[dict[str, Any]] = []
            stale_stop = False
            for entry in entries:
                posted = entry.get("posted_date")
                if posted is not None and posted < cutoff:
                    logger.info(
                        "virtualvocations first stale lastmod — "
                        "stopping newest-first walk"
                    )
                    stale_stop = True
                    break
                if not _is_engineering_title(entry["title"]):
                    continue
                page_jobs.append(entry)
            logger.info(
                f"virtualvocations sitemap: {len(entries)} urls, "
                f"{len(page_jobs)} engineering in-window"
                f"{' (stale stop)' if stale_stop else ''}"
            )
            self._emit_listings(page_jobs, kept)
        except Exception as e:
            logger.error(f"Error fetching jobs from VirtualVocations: {e}")
            logger.debug(traceback.format_exc())
        logger.info(
            f"Successfully fetched {len(kept)} jobs from virtualvocations"
        )
        return kept

    def _emit_listings(
        self,
        page_jobs: list[dict[str, Any]],
        kept: list[dict[str, Any]],
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
        fetched = 0
        for job in pending:
            if profile and exclusion_reason(_inclusion_fields(job), profile):
                skipped += 1
                continue
            html = _fetch_detail_html(job["listing_url"])
            fetched += 1
            if html:
                _merge_detail(job, html)
                if profile and exclusion_reason(_inclusion_fields(job), profile):
                    skipped += 1
                    continue
            self._emit(job, kept)
            if fetched < len(pending):
                time.sleep(_FETCH_DELAY)
        if skipped:
            logger.info(
                "virtualvocations skipped "
                f"{skipped} ineligible listings before persist"
            )
        remember_listing_urls(
            self.source_name, [job["listing_url"] for job in pending]
        )

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
    return _plain(slug.replace("-", " ")).title()


def _parse_job_url(url: str) -> dict[str, str] | None:
    match = _JOB_PATH_RE.match((url or "").strip())
    if not match:
        return None
    slug, job_id = match.group(1), match.group(2)
    title = _title_from_slug(slug)
    if not title or not job_id:
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
    }


def _parse_sitemap(content: bytes) -> list[dict[str, Any]]:
    """Document-order job URLs. Newest-first is live sitemap order, not a sort."""
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
    return jobs


def _fetch_sitemap() -> list[dict[str, Any]]:
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(
                SITEMAP_URL, headers=_SITEMAP_HEADERS, timeout=_SITEMAP_TIMEOUT
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"virtualvocations sitemap failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After") or "60"
            logger.info(
                f"virtualvocations sitemap HTTP 429 Retry-After={retry_after} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"virtualvocations sitemap HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return _parse_sitemap(resp.content)
    logger.info(f"virtualvocations sitemap skipped after {_RETRIES} attempts")
    return []


def _merge_detail(job: dict[str, Any], html: str) -> None:
    title_m = _H1_RE.search(html)
    title = _plain(title_m.group(1)) if title_m else ""
    if title:
        job["title"] = title
    loc_m = _LOCATION_RE.search(html)
    if loc_m:
        location = _plain(loc_m.group(1))
        if location:
            job["location"] = location
    summary_m = _SUMMARY_RE.search(html)
    if summary_m:
        summary = _plain(summary_m.group(1))
        if summary:
            job["description"] = summary


def _fetch_detail_html(url: str) -> str | None:
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(
                url,
                headers=_HEADERS,
                cookies=_COOKIES,
                timeout=_DETAIL_TIMEOUT,
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"virtualvocations detail failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code == 429:
            logger.info(
                f"virtualvocations detail HTTP 429 attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"virtualvocations detail HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.text or ""
    logger.info(f"virtualvocations detail skipped after {_RETRIES} attempts")
    return None
