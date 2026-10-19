"""
Remote100K connector.

Fetches $100K+ remote job listings from https://remote100k.com/ via their
sitemap.xml and JSON-LD structured data embedded on each job page.

Strategy
--------
1. Parse sitemap.xml (handles both flat <urlset> and <sitemapindex>).
2. Filter to /remote-job/ URLs with engineering-relevant slug keywords.
3. The live sitemap has no ``lastmod`` but is newest-first (JSON-LD
   ``datePosted``). Walk unseen engineering URLs in document order, drop
   stale rows via ``job_age_cutoff``, and stop at the first stale job.
4. Extract the JobPosting JSON-LD block (quoted or unquoted type), then
   the direct ATS apply URL, stripping ``?ref=remote100k``.
5. Store the ATS URL so detect_ats() classifies it and prefill goes
   straight to the application form.
"""
from __future__ import annotations

import json
import re
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("remote100k_connector")

_SITEMAP_URL = "https://remote100k.com/sitemap.xml"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,*/*",
}

# Live sitemap has no lastmod; datePosted walk on 2026-09-14 was newest-first.
_SITEMAP_NEWEST_FIRST = True
_FETCH_DELAY = 0.4
_API_TIMEOUT = 40
_RETRIES = 3
_RETRY_DELAY = 1.5

_LD_SCRIPT_RE = re.compile(
    r"<script([^>]*)>(.*?)</script>",
    re.DOTALL | re.IGNORECASE,
)
_LD_TYPE_RE = re.compile(
    r"""type\s*=\s*['"]?application/ld\+json['"]?""",
    re.IGNORECASE,
)

_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# ATS domains whose URLs we extract directly from the page HTML.
_ATS_DOMAINS = (
    "jobs.ashbyhq.com",
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.lever.co",
    "apply.workable.com",
    "jobs.smartrecruiters.com",
    "app.dover.com",
    "recruiting.paylocity.com",
)

_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data scientist", "machine learning", "ml ", " ml", "ai ", " ai", "mlops",
    "python", "typescript", "golang", "rust", "java", "deep learning",
    "llm ", " llm", "artificial intelligence", "agentic", "rag",
}


class Remote100kConnector(BaseConnector):
    def __init__(self):
        self.source_name = "remote100k"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        logger.info(
            f"Fetching jobs from remote100k.com sitemap (age_days={age_days})…"
        )
        content = _fetch_bytes(_SITEMAP_URL, "sitemap")
        if content is None:
            return []
        try:
            urls, has_lastmod = _parse_sitemap(content)
        except Exception as e:
            logger.info(f"remote100k sitemap parse failed ({type(e).__name__})")
            logger.debug(traceback.format_exc())
            return []

        newest_first = has_lastmod or _SITEMAP_NEWEST_FIRST
        eng_urls = [u for u in urls if _is_engineering_url(u)]
        to_fetch = unseen_listing_urls(eng_urls, self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        logger.info(
            f"Sitemap: {len(urls)} job URLs total, "
            f"{len(eng_urls)} match engineering keywords, "
            f"newest_first={newest_first}, fetching {len(to_fetch)}"
        )
        if not to_fetch:
            logger.info(
                "No unseen Remote100K listing URLs; "
                "already crawled or stored. --initial only widens the age window."
            )
            return []

        jobs: list[dict[str, Any]] = []
        crawled: list[str] = []
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
                            if newest_first:
                                logger.info(
                                    "remote100k hit first stale job — stopping"
                                )
                                break
                            raw = None
                    if raw:
                        self._emit(raw, jobs)
                time.sleep(_FETCH_DELAY)
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                logger.debug(traceback.format_exc())
        remember_listing_urls(self.source_name, crawled)

        logger.info(f"Successfully fetched {len(jobs)} jobs from remote100k.com")
        return jobs

    def normalize(self, raw_job: dict[str, Any]) -> dict[str, Any]:
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
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_sitemap(content: bytes) -> tuple[list[str], bool]:
    """Return (/remote-job/ URLs, newest_first) from a urlset or sitemap index.

    Prefix-capping from this parser is valid only when ``lastmod`` is present.
    Missing lastmod leaves document order and ``newest_first=False``; fetch_jobs
    may still treat the board as newest-first via ``_SITEMAP_NEWEST_FIRST``.
    """
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return [], False

    tag = root.tag.lower()

    # Sitemap index — follow child sitemaps to find job URLs.
    if "sitemapindex" in tag:
        entries: list[tuple[str, str]] = []
        has_lastmod = False
        for sm_el in root.findall("sm:sitemap", _NS) or root.findall("sitemap"):
            child_loc = (
                sm_el.findtext("sm:loc", namespaces=_NS)
                or sm_el.findtext("loc")
                or ""
            ).strip()
            if not child_loc:
                continue
            try:
                child = _fetch_bytes(child_loc, "child sitemap")
                if child is None:
                    continue
                child_entries, child_lastmod = _urlset_entries(ET.fromstring(child))
                entries.extend(child_entries)
                has_lastmod = has_lastmod or child_lastmod
                time.sleep(0.2)
            except Exception:
                continue
        return _finalize_sitemap_entries(entries, has_lastmod)

    entries, has_lastmod = _urlset_entries(root)
    return _finalize_sitemap_entries(entries, has_lastmod)


def _urlset_entries(root: ET.Element) -> tuple[list[tuple[str, str]], bool]:
    entries: list[tuple[str, str]] = []
    has_lastmod = False
    for url_el in root.findall("sm:url", _NS) or root.findall("url"):
        loc = (
            url_el.findtext("sm:loc", namespaces=_NS)
            or url_el.findtext("loc")
            or ""
        ).strip()
        if not re.match(r"https://remote100k\.com/remote-job/[^/]+/?$", loc):
            continue
        lastmod = (
            url_el.findtext("sm:lastmod", namespaces=_NS)
            or url_el.findtext("lastmod")
            or ""
        ).strip()
        if lastmod:
            has_lastmod = True
        entries.append((lastmod, loc))
    return entries, has_lastmod


def _finalize_sitemap_entries(
    entries: list[tuple[str, str]], has_lastmod: bool
) -> tuple[list[str], bool]:
    if has_lastmod:
        entries = sorted(entries, key=lambda x: x[0], reverse=True)
    return [loc for _, loc in entries], has_lastmod


def _is_engineering_url(url: str) -> bool:
    slug = url.rstrip("/").split("/")[-1].lower().replace("-", " ")
    blob = f" {slug} "
    return any(kw in blob for kw in _ENGINEERING_KEYWORDS)


def _fetch_bytes(url: str, label: str) -> bytes | None:
    """GET bytes with retries; None after timeout/connection/HTTP failures."""
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_API_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"remote100k {label} failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"remote100k {label} HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.content
    logger.info(f"remote100k {label} skipped after {_RETRIES} attempts")
    return None


def _fetch_job_page(url: str) -> dict[str, Any] | None:
    html = _fetch_detail_html(url)
    if html is None:
        return None
    return _extract_job(html, url)


def _fetch_detail_html(url: str) -> str | None:
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_API_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"remote100k detail failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code >= 400:
            logger.info(
                f"remote100k detail HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.text or ""
    logger.info(f"remote100k detail skipped after {_RETRIES} attempts")
    return None


def _extract_job(html: str, page_url: str) -> dict[str, Any] | None:
    """Parse JSON-LD and extract ATS apply URL from page HTML."""
    # --- JSON-LD ---
    jsonld: dict[str, Any] = {}
    for match in _LD_SCRIPT_RE.finditer(html):
        attrs, raw = match.group(1), match.group(2)
        if not _LD_TYPE_RE.search(attrs):
            continue
        try:
            data = json.loads(raw.strip())
        except Exception:
            continue
        if data.get("@type") == "JobPosting":
            jsonld = data
            break

    if not jsonld:
        return None

    title = (jsonld.get("title") or "").strip()
    company = ((jsonld.get("hiringOrganization") or {}).get("name") or "Unknown").strip()
    description = (jsonld.get("description") or "").strip()

    # Salary from baseSalary block → append to description so it shows in UI.
    bs = jsonld.get("baseSalary") or {}
    bsv = bs.get("value") or {}
    lo, hi, cur = bsv.get("minValue"), bsv.get("maxValue"), bs.get("currency", "")
    if lo or hi:
        salary_str = f"{cur}{lo:,.0f}–{cur}{hi:,.0f}" if lo and hi else f"{cur}{lo or hi:,.0f}"
        if salary_str not in description:
            description = f"{salary_str}\n\n{description}".strip()

    # Location: derive from jobLocationType + description text.
    loc_type = (jsonld.get("jobLocationType") or "").upper()
    location = "Worldwide" if loc_type == "TELECOMMUTE" else "Remote"

    posted_date = None
    date_str = jsonld.get("datePosted")
    if date_str:
        try:
            posted_date = dateutil_parser.parse(date_str)
            if posted_date.tzinfo is None:
                posted_date = posted_date.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # --- ATS apply URL ---
    # The apply URL appears in an <a href="..."> tag in the page source.
    # Strip the ?ref=remote100k tracking parameter before storing.
    apply_url = _extract_ats_url(html) or page_url

    slug = page_url.rstrip("/").split("/")[-1]
    return {
        "id": slug,
        "url": apply_url,
        "title": title,
        "company": company,
        "location": location,
        "description": description,
        "posted_date": posted_date,
    }


def _extract_ats_url(html: str) -> str | None:
    """Find a direct ATS apply URL in the page HTML and strip tracking params."""
    for domain in _ATS_DOMAINS:
        pattern = rf'href=["\']({re.escape("https://" + domain)}[^"\']*)["\']'
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            return _strip_ref_param(m.group(1))
    return None


def _strip_ref_param(url: str) -> str:
    """Remove ?ref= and ?ref=remote100k tracking parameters from a URL."""
    parsed = urlparse(url)
    qs = re.sub(r"(?:^|&)ref=[^&]*", "", parsed.query).strip("&")
    return urlunparse(parsed._replace(query=qs))
