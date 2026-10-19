"""
Y Combinator public jobs connector.

Fetches the guest listing at
https://www.ycombinator.com/jobs/role/software-engineer/remote

The page is Inertia SSR (``data-page`` JSON). There is no RSS. Dates on the
cards are mixed, and the guest list is truncated ("Create a profile to see
more"). Walk that single page, keep engineering titles, skip known URLs.
Do not age-filter: the public set is already small. Detail-fetch and emit
each unseen job before the next so an abort still stores those jobs.

Apply goes through a YC account, so scoring caps this source at review.
The logged-in Work at a Startup directory is ``connectors/waas.py``.
"""
from __future__ import annotations

import html as html_lib
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import requests

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

logger = setup_logger("ycombinator_connector")

BASE_URL = "https://www.ycombinator.com"
LISTING_URL = f"{BASE_URL}/jobs/role/software-engineer/remote"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
_FETCH_DELAY = 0.4
_API_TIMEOUT = 40
_RETRIES = 3
_RETRY_DELAY = 1.5

_DATA_PAGE_RE = re.compile(r'data-page="([^"]*)"', re.IGNORECASE)
_RELATIVE_RE = re.compile(
    r"(?:about|almost)?\s*(\d+)\s+(minutes?|hours?|days?|weeks?|months?|years?)\b",
    re.IGNORECASE,
)
_UNIT_TO_KWARG = {
    "minute": "minutes",
    "minutes": "minutes",
    "hour": "hours",
    "hours": "hours",
    "day": "days",
    "days": "days",
    "week": "weeks",
    "weeks": "weeks",
}

_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}


class YCombinatorConnector(BaseConnector):
    def __init__(self):
        self.source_name = "ycombinator"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        logger.info("Fetching jobs from ycombinator.com public software-engineer remote list…")
        listing_html = _fetch_html(LISTING_URL)
        if listing_html is None:
            return []

        parsed: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item in _extract_listing_jobs(listing_html):
            raw = _parse_listing_job(item)
            if not raw:
                continue
            if raw["id"] in seen_ids:
                continue
            seen_ids.add(raw["id"])
            parsed.append(raw)

        urls = [job["url"] for job in parsed]
        to_fetch = unseen_listing_urls(urls, self.source_name)
        wanted = set(to_fetch)
        jobs = [job for job in parsed if job["url"] in wanted]
        logger.info(
            f"YC listing: {len(parsed)} engineering cards, fetching {len(jobs)} unseen"
        )

        crawled: list[str] = []
        kept_jobs: list[dict[str, Any]] = []
        for i, job in enumerate(jobs):
            detail_html = _fetch_html(job["url"])
            if detail_html is None:
                logger.info(
                    f"YC detail skipped for {job['url']} — keeping listing fields"
                )
            else:
                _merge_detail(job, detail_html)
            crawled.append(job["url"])
            self._emit(job, kept_jobs)
            remember_listing_urls(self.source_name, [job["url"]])
            if i + 1 < len(jobs):
                time.sleep(_FETCH_DELAY)

        if not crawled:
            remember_listing_urls(self.source_name, crawled)
        logger.info(f"Successfully fetched {len(kept_jobs)} jobs from ycombinator")
        return kept_jobs

    def normalize(self, raw_job: dict[str, Any]) -> dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        location = raw_job.get("location") or "Remote"
        if not isinstance(location, str):
            location = _location_text(location)

        return {
            "external_id": raw_job.get("id") or url,
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


def _fetch_html(url: str) -> str | None:
    short = url if len(url) < 80 else url[:77] + "..."
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_API_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"ycombinator GET failed ({type(e).__name__}) {short} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"ycombinator GET HTTP {resp.status_code} {short} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.text
    logger.info(f"ycombinator GET skipped after {_RETRIES} attempts: {short}")
    return None


def _inertia_payload(html: str) -> dict[str, Any]:
    match = _DATA_PAGE_RE.search(html or "")
    if not match:
        return {}
    try:
        data = json.loads(html_lib.unescape(match.group(1)))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _extract_listing_jobs(html: str) -> list[dict[str, Any]]:
    props = _inertia_payload(html).get("props") or {}
    postings = props.get("jobPostings")
    if not isinstance(postings, list):
        return []
    return [item for item in postings if isinstance(item, dict)]


def _is_engineering_title(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in _ENGINEERING_KEYWORDS)


def _location_text(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, dict):
        for key in ("name", "addressLocality", "addressRegion", "address"):
            text = _location_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        names = [_location_text(v) for v in value if v]
        names = [n for n in names if n]
        return ", ".join(names)
    return ""


def _parse_relative_date(text: str, now: datetime | None = None) -> datetime | None:
    match = _RELATIVE_RE.search(text or "")
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2).lower()
    now = now or datetime.now(tz=timezone.utc)
    kwarg = _UNIT_TO_KWARG.get(unit)
    if kwarg:
        return now - timedelta(**{kwarg: amount})
    if unit in ("month", "months"):
        return now - timedelta(days=30 * amount)
    if unit in ("year", "years"):
        return now - timedelta(days=365 * amount)
    return None


def _listing_url(item: dict[str, Any]) -> str:
    path = (item.get("url") or "").strip()
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if path.startswith("/"):
        return urljoin(BASE_URL, path)
    job_id = item.get("id")
    slug = (item.get("companySlug") or "").strip()
    if slug and job_id:
        return f"{BASE_URL}/companies/{slug}/jobs/{job_id}"
    return ""


def _listing_description(item: dict[str, Any]) -> str:
    chunks: list[str] = []
    one_liner = (item.get("companyOneLiner") or "").strip()
    if one_liner:
        chunks.append(one_liner)
    salary = (item.get("salaryRange") or "").strip()
    if salary:
        chunks.append(f"Salary: {salary}")
    equity = (item.get("equityRange") or "").strip()
    if equity:
        chunks.append(f"Equity: {equity}")
    skills = item.get("skills")
    if isinstance(skills, list):
        names = [str(s).strip() for s in skills if s]
        if names:
            chunks.append("Skills: " + ", ".join(names))
    return "\n".join(chunks)


def _parse_listing_job(item: dict[str, Any]) -> dict[str, Any] | None:
    title = (item.get("title") or "").strip()
    if not title or not _is_engineering_title(title):
        return None
    url = _listing_url(item)
    if not url:
        return None
    job_id = str(item.get("id") or url)
    location = _location_text(item.get("location")) or "Remote"
    return {
        "id": job_id,
        "title": title,
        "company": (item.get("companyName") or "").strip() or "Unknown",
        "url": url,
        "description": _listing_description(item),
        "location": location,
        "posted_date": _parse_relative_date(str(item.get("createdAt") or "")),
        "last_active": str(item.get("lastActive") or ""),
    }


def _merge_detail(job: dict[str, Any], html: str) -> None:
    props = _inertia_payload(html).get("props") or {}
    detail = props.get("job")
    if not isinstance(detail, dict):
        return
    description = (detail.get("description") or "").strip()
    if description:
        extra = _listing_description(detail)
        job["description"] = description if not extra else f"{description}\n\n{extra}"
    loc = _location_text(detail.get("location")) or ""
    if loc:
        job["location"] = loc
    company = (detail.get("companyName") or "").strip()
    if company:
        job["company"] = company
    title = (detail.get("title") or "").strip()
    if title:
        job["title"] = title
    created = _parse_relative_date(str(detail.get("createdAt") or ""))
    if created:
        job["posted_date"] = created
