"""
Flexa Careers connector.

Fetches job listings from https://flexa.careers via their GraphQL API
and JSON-LD structured data on individual job pages.

Strategy
--------
1. Query the GraphQL API with ``jobs(limit=N, sort: DATE_DESC)`` to retrieve
   the most recent listings with title, location, and company name.
2. Filter to engineering-relevant jobs by title keyword.
3. Fetch each engineering job page for JSON-LD description / ``datePosted``.
4. Stop at the first stale ``datePosted`` (DATE_DESC). Skip expired ``validThrough``.
5. Return the Flexa page URL as the job URL — the prefill system will open it,
   find the employer apply link via ``extract_apply_url``, and navigate to the
   real ATS.
"""
from __future__ import annotations

import json
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

logger = setup_logger("flexa_connector")

_GRAPHQL_URL = "https://flexa.careers/api/graphql"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Accept": "application/json",
}
_PAGE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# GraphQL jobs to fetch per request (API limit unknown — 300 is conservative).
_GRAPHQL_BATCH = 300
_FETCH_DELAY = 0.4
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

_GRAPHQL_QUERY = """
{
  jobs(limit: %d, sort: DATE_DESC) {
    id
    title
    url
    location
    company { name }
  }
}
"""


class FlexaConnector(BaseConnector):
    def __init__(self):
        self.source_name = "flexa"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            "Fetching jobs from flexa.careers GraphQL API "
            f"(DATE_DESC, age_days={age_days}; stop at first stale job)…"
        )

        raw_jobs = _fetch_graphql_jobs(_GRAPHQL_BATCH)
        logger.info(f"GraphQL returned {len(raw_jobs)} jobs total")

        eng_jobs = [j for j in raw_jobs if _is_engineering_title(j.get("title", ""))]
        logger.info(f"{len(eng_jobs)} match engineering keywords")

        jobs: List[Dict[str, Any]] = []
        for gql_job in eng_jobs:
            url = (gql_job.get("url") or "").strip()
            try:
                enriched = _enrich_from_page(gql_job)
                if enriched:
                    posted = enriched.get("posted_date")
                    if posted:
                        if posted.tzinfo is None:
                            posted = posted.replace(tzinfo=timezone.utc)
                        if posted < cutoff:
                            logger.info("flexa first stale job — stopping")
                            break
                    self._emit(enriched, jobs)
                time.sleep(_FETCH_DELAY)
            except Exception as e:
                logger.info(f"flexa detail skip ({type(e).__name__}) for {url}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(jobs)} jobs from flexa.careers")
        return jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        location = _stringify_location(raw_job.get("location") or "Worldwide")

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
            # flexa.careers is a listing page; ats_type resolved at prefill time.
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name


def _fetch_graphql_jobs(limit: int) -> List[Dict[str, Any]]:
    """Query the Flexa GraphQL API and return raw job dicts."""
    payload = {"query": _GRAPHQL_QUERY % limit}
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.post(
                _GRAPHQL_URL, json=payload, headers=_HEADERS, timeout=_API_TIMEOUT
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"flexa GraphQL failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"flexa GraphQL HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        try:
            data = resp.json()
        except Exception as e:
            logger.info(f"flexa GraphQL JSON failed ({type(e).__name__})")
            return []
        jobs = (data.get("data") or {}).get("jobs") or []
        return [j for j in jobs if isinstance(j, dict)]
    logger.info(f"flexa GraphQL skipped after {_RETRIES} attempts")
    return []


def _is_engineering_title(title: str) -> bool:
    """Return True if the job title contains an engineering-relevant keyword."""
    t = title.lower()
    return any(kw in t for kw in _ENGINEERING_KEYWORDS)


def _stringify_location(value: Any) -> str:
    """Turn GraphQL strings or JSON-LD Place/PostalAddress objects into text."""
    if value is None or value == "":
        return "Worldwide"
    if isinstance(value, str):
        return value.strip() or "Worldwide"
    if isinstance(value, list):
        parts = [_stringify_location(v) for v in value]
        parts = [p for p in parts if p and p != "Worldwide"]
        return ", ".join(parts) or "Worldwide"
    if isinstance(value, dict):
        nested = value.get("address")
        if nested is not None:
            nested_text = _stringify_location(nested)
            if nested_text != "Worldwide":
                return nested_text
        parts = [
            str(value[k]).strip()
            for k in ("addressLocality", "addressRegion", "addressCountry")
            if value.get(k)
        ]
        if parts:
            return ", ".join(parts)
        name = value.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return "Worldwide"


def _fetch_detail_html(url: str) -> str | None:
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(url, headers=_PAGE_HEADERS, timeout=_API_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"flexa detail failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"flexa detail HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.text or ""
    logger.info(f"flexa detail skipped after {_RETRIES} attempts")
    return None


def _enrich_from_page(gql_job: Dict[str, Any]) -> Dict[str, Any] | None:
    """Fetch the Flexa job page and merge JSON-LD data into the GraphQL job dict."""
    url = (gql_job.get("url") or "").strip()
    if not url:
        return None

    html = _fetch_detail_html(url)
    if html is None:
        return None

    jsonld = _extract_jsonld(html)

    if jsonld:
        valid_through = jsonld.get("validThrough")
        if valid_through:
            try:
                vt = dateutil_parser.parse(valid_through)
                if vt.tzinfo is None:
                    vt = vt.replace(tzinfo=timezone.utc)
                if vt < datetime.now(tz=timezone.utc):
                    return None
            except Exception:
                pass

    posted_date = None
    date_str = (jsonld or {}).get("datePosted")
    if date_str:
        try:
            posted_date = dateutil_parser.parse(date_str)
            if posted_date.tzinfo is None:
                posted_date = posted_date.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    description = (jsonld or {}).get("description", "")
    company_name = (
        (gql_job.get("company") or {}).get("name")
        or ((jsonld or {}).get("hiringOrganization") or {}).get("name")
        or "Unknown"
    )
    location = _stringify_location(
        gql_job.get("location")
        or ((jsonld or {}).get("jobLocation") or {}).get("address")
        or "Worldwide"
    )

    return {
        "id": gql_job.get("id", ""),
        "url": url,
        "title": gql_job.get("title", ""),
        "company": company_name,
        "location": location,
        "description": description,
        "posted_date": posted_date,
    }


def _extract_jsonld(html: str) -> Dict[str, Any] | None:
    """Parse the first JobPosting JSON-LD block from page HTML."""
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(match.group(1).strip())
        except Exception:
            continue
        if data.get("@type") == "JobPosting":
            return data
    return None
