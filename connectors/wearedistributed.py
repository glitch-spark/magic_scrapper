"""
We Are Distributed jobs connector.

Fetches job listings from https://wearedistributed.org/jobs via sitemap.xml
and JSON-LD structured data embedded on each individual job page.

Strategy
--------
1. Parse sitemap.xml to collect all ``/job/<slug>`` URLs (newest-first by
   ``lastmod``).
2. Filter to engineering-relevant slugs (keyword substring match).
3. Skip known listing URLs, then fetch unseen pages and extract JSON-LD.
4. Stop at the first stale ``datePosted`` (newest-first). Skip expired ``validThrough``.
5. Return the wearedistributed.org page URL as the job URL — the prefill
   system will open it, find the employer apply link via ``extract_apply_url``,
   and navigate to the real ATS.
"""
from __future__ import annotations

import json
import re
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

logger = setup_logger("wearedistributed_connector")

_SITEMAP_URL = "https://wearedistributed.org/sitemap.xml"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,*/*",
}

_FETCH_DELAY = 0.5
_API_TIMEOUT = 40
_RETRIES = 3
_RETRY_DELAY = 1.5

_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}

_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class WeAreDistributedConnector(BaseConnector):
    def __init__(self):
        self.source_name = "wearedistributed"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            "Fetching jobs from wearedistributed.org sitemap "
            f"(newest-first lastmod, age_days={age_days}; "
            "stop at first stale job)…"
        )
        content = _fetch_bytes(_SITEMAP_URL, "sitemap")
        if content is None:
            return []
        try:
            urls = _parse_sitemap(content)
        except Exception as e:
            logger.info(f"wearedistributed sitemap parse failed ({type(e).__name__})")
            logger.debug(traceback.format_exc())
            return []

        eng_urls = [u for u in urls if _is_engineering_url(u)]
        to_fetch = unseen_listing_urls(eng_urls, self.source_name)
        logger.info(
            f"Sitemap: {len(urls)} job URLs total, "
            f"{len(eng_urls)} match engineering keywords, "
            f"fetching {len(to_fetch)}"
        )

        jobs: List[Dict[str, Any]] = []
        crawled: List[str] = []
        for url in to_fetch:
            try:
                raw = _fetch_job_page(url)
                crawled.append(url)
                if raw:
                    posted = raw.get("posted_date")
                    if posted:
                        if posted.tzinfo is None:
                            posted = posted.replace(tzinfo=timezone.utc)
                        if posted < cutoff:
                            logger.info(
                                "wearedistributed first stale job — stopping"
                            )
                            break
                    self._emit(raw, jobs)
                time.sleep(_FETCH_DELAY)
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                logger.debug(traceback.format_exc())
        remember_listing_urls(self.source_name, crawled)

        logger.info(f"Successfully fetched {len(jobs)} jobs from wearedistributed.org")
        return jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        location = raw_job.get("location") or "Worldwide"

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
            # wearedistributed.org is a listing page; ats_type resolved at prefill time.
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name


def _parse_sitemap(content: bytes) -> List[str]:
    """Return /job/ page URLs from the sitemap, newest first by lastmod."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    entries: List[tuple[str, str]] = []  # (lastmod, url)
    for url_el in root.findall("sm:url", _NS) or root.findall("url"):
        loc = (
            (url_el.findtext("sm:loc", namespaces=_NS) or url_el.findtext("loc") or "")
            .strip()
        )
        if not re.match(r"https://wearedistributed\.org/job/[^/]+$", loc):
            continue
        lastmod = (
            url_el.findtext("sm:lastmod", namespaces=_NS)
            or url_el.findtext("lastmod")
            or ""
        )
        entries.append((lastmod, loc))

    entries.sort(key=lambda x: x[0], reverse=True)
    return [loc for _, loc in entries]


def _is_engineering_url(url: str) -> bool:
    """Return True if the URL slug contains an engineering-relevant keyword."""
    slug = url.rstrip("/").split("/")[-1].lower().replace("-", " ")
    blob = f" {slug} "
    return any(kw in blob for kw in _ENGINEERING_KEYWORDS)


def _fetch_bytes(url: str, label: str) -> bytes | None:
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_API_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"wearedistributed {label} failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"wearedistributed {label} HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.content
    logger.info(f"wearedistributed {label} skipped after {_RETRIES} attempts")
    return None


def _fetch_job_page(url: str) -> Dict[str, Any] | None:
    html = _fetch_detail_html(url)
    if html is None:
        return None
    return _extract_jsonld(html, url)


def _fetch_detail_html(url: str) -> str | None:
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_API_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"wearedistributed detail failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"wearedistributed detail HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.text or ""
    logger.info(f"wearedistributed detail skipped after {_RETRIES} attempts")
    return None


def _extract_jsonld(html: str, page_url: str) -> Dict[str, Any] | None:
    """Parse a JobPosting JSON-LD block from page HTML and return a raw job dict."""
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(match.group(1).strip())
        except Exception:
            continue

        if data.get("@type") != "JobPosting":
            continue

        valid_through = data.get("validThrough")
        if valid_through:
            try:
                vt = dateutil_parser.parse(valid_through)
                if vt.tzinfo is None:
                    vt = vt.replace(tzinfo=timezone.utc)
                if vt < datetime.now(tz=timezone.utc):
                    return None
            except Exception:
                pass

        title = (data.get("title") or "").strip()
        company = ((data.get("hiringOrganization") or {}).get("name") or "Unknown").strip()
        description = (data.get("description") or "").strip()

        location = "Worldwide"
        loc_reqs = data.get("applicantLocationRequirements")
        if isinstance(loc_reqs, list):
            names = [r.get("name", "") for r in loc_reqs if r.get("name")]
            if names:
                location = ", ".join(names)
        elif isinstance(loc_reqs, dict) and loc_reqs.get("name"):
            location = loc_reqs["name"]

        posted_date = None
        date_str = data.get("datePosted")
        if date_str:
            try:
                posted_date = dateutil_parser.parse(date_str)
                if posted_date.tzinfo is None:
                    posted_date = posted_date.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        slug = page_url.rstrip("/").split("/")[-1]
        return {
            "id": slug,
            "url": page_url,
            "title": title,
            "company": company,
            "location": location,
            "description": description,
            "posted_date": posted_date,
        }

    return None
