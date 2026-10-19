"""
Work at a Startup (WAAS) connector.

Fetches the logged-in companies directory at
https://www.workatastartup.com/companies?demographic=any&hasEquity=any&hasSalary=any&industry=any&interviewProcess=any&jobType=fulltime&layout=list-compact&remote=yes&remote=only&role=eng&role_type=be&role_type=data_sci&role_type=devops&role_type=fe&role_type=fs&role_type=ml&sortBy=created_desc&tab=any&usVisaNotRequired=any

Playwright login with ``WAAS_EMAIL`` / ``WAAS_PASSWORD`` (``YC_EMAIL`` /
``YC_PASSWORD`` also accepted). Missing credentials → skip.

The directory loads in two steps, both driven by infinite scroll
(``ThrottledInfiniteScroll`` increments the Algolia page):
1. Logged-in Algolia ``search`` — hits are ``company_id`` only
2. ``POST /companies/fetch`` with those ids hydrates companies and ``jobs[]``

We open the logged-in URL, wait for that first Algolia search, hydrate via
``/companies/fetch``, then replay remaining Algolia pages (same query, next
page) and fetch each new id batch. Newest-first: stop at a stale batch.

Apply goes through a YC account, so scoring caps this source at review.
The public truncated YC list is ``connectors/ycombinator.py``.
"""
from __future__ import annotations

import html as html_lib
import json
import os
import re
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import requests
from dateutil import parser as dateutil_parser
from dotenv import load_dotenv

from connectors.base import BaseConnector
from utils.alerts import ScrapeHalted, halt
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff
from utils.job_store import remember_listing_urls, unseen_listing_urls
from utils.logger import setup_logger
from utils.text_cleaning import clean_description, sanitize_skill_object_dumps

load_dotenv()

logger = setup_logger("waas_connector")

YC_BASE = "https://www.ycombinator.com"
WAAS_BASE = "https://www.workatastartup.com"
WAAS_COMPANIES_URL = (
    "https://www.workatastartup.com/companies?demographic=any&hasEquity=any"
    "&hasSalary=any&industry=any&interviewProcess=any&jobType=fulltime"
    "&layout=list-compact&remote=yes&remote=only&role=eng"
    "&role_type=be&role_type=data_sci&role_type=devops&role_type=fe"
    "&role_type=fs&role_type=ml&sortBy=created_desc&tab=any"
    "&usVisaNotRequired=any"
)
ACCOUNT_LOGIN_URL = "https://account.ycombinator.com/"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
_PUBLIC_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
_FETCH_DELAY = 0.4
_NAV_TIMEOUT_MS = 60_000
_SCROLL_WAIT_MS = 8_000
_GOTO_RETRIES = 3
_GOTO_RETRY_DELAY = 1.5
# Newest-first directory: stale-batch stop; these are runaway guards.
_MAX_PAGES = 80
_MAX_SCROLLS = 80
_HITS_PER_PAGE = 10
_ALGOLIA_HEADER_KEYS = {
    "accept",
    "content-type",
    "x-algolia-agent",
    "x-algolia-api-key",
    "x-algolia-application-id",
}
_ALGOLIA_HOST_MARKERS = ("algolia.net", "algolianet.com", "algolia.io")

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

_LOGGED_RAW_JOB_KEYS = False


class WaasConnector(BaseConnector):
    def __init__(self):
        self.source_name = "waas"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        email, password = _credentials()
        if not email or not password:
            logger.error(
                "WAAS_EMAIL / WAAS_PASSWORD not set in .env — skipping"
            )
            return []

        logger.info("Fetching jobs from Work at a Startup (newest jobs)…")
        cutoff = job_age_cutoff(self.source_name)
        all_jobs: list[dict[str, Any]] = []
        remembered: list[str] = []

        try:
            with _browser_session(email, password) as page:
                if page is None:
                    return []
                companies = _collect_companies(page, cutoff)
                all_jobs, dated = _jobs_from_companies(companies, cutoff)
                dated_kept = sum(1 for job in all_jobs if job.get("posted_date"))
                urls = [job["url"] for job in all_jobs]
                unseen = set(unseen_listing_urls(urls, self.source_name))
                all_jobs = [job for job in all_jobs if job["url"] in unseen]
                logger.info(
                    f"WAAS directory: {len(companies)} companies, "
                    f"{len(all_jobs)} unseen engineering jobs "
                    f"({dated_kept} dated, {len(dated)} date samples)"
                )
                _enrich_details(page, all_jobs, on_job=self._emit)
                remembered = [job["url"] for job in all_jobs]
        except ScrapeHalted:
            raise
        except Exception as e:
            logger.info(f"WAAS fetch aborted ({type(e).__name__}): {e}")
            logger.debug(traceback.format_exc())
            if all_jobs and not remembered:
                remembered = [job["url"] for job in all_jobs]
            if remembered:
                remember_listing_urls(self.source_name, remembered)
            logger.info(f"Successfully fetched {len(all_jobs)} jobs from waas")
            return all_jobs

        if remembered:
            remember_listing_urls(self.source_name, remembered)
        logger.info(f"Successfully fetched {len(all_jobs)} jobs from waas")
        return all_jobs

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


def _goto(page: Any, url: str, *, label: str = "page") -> Any | None:
    """Navigate with retries. Returns the response, or None after exhausting retries."""
    last_err: Exception | None = None
    for attempt in range(1, _GOTO_RETRIES + 1):
        try:
            return page.goto(
                url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS
            )
        except Exception as e:
            last_err = e
            logger.info(
                f"WAAS goto {label} failed ({type(e).__name__}) "
                f"attempt {attempt}/{_GOTO_RETRIES}"
            )
            if attempt < _GOTO_RETRIES:
                time.sleep(_GOTO_RETRY_DELAY * attempt)
    if last_err is not None:
        logger.info(
            f"WAAS goto {label} skipped after {_GOTO_RETRIES} attempts: {last_err}"
        )
    return None


def _url_host(url: str) -> str:
    return (urlparse(url or "").hostname or "").lower()


def _is_waas_host(url: str) -> bool:
    host = _url_host(url)
    return host == "workatastartup.com" or host.endswith(".workatastartup.com")


def _on_companies_directory(url: str) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").rstrip("/")
    return _is_waas_host(url) and path.endswith("/companies")


def _credentials() -> tuple[str, str]:
    load_dotenv()
    email = (
        os.getenv("WAAS_EMAIL", "").strip()
        or os.getenv("YC_EMAIL", "").strip()
    )
    password = (
        os.getenv("WAAS_PASSWORD", "").strip()
        or os.getenv("YC_PASSWORD", "").strip()
    )
    return email, password


_COMPANIES_FETCH_JS = """
async (ids) => {
    const csrf = document.querySelector('meta[name="csrf-token"]')
        ?.getAttribute('content') || '';
    const res = await fetch('/companies/fetch', {
        method: 'POST',
        credentials: 'include',
        cache: 'no-store',
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRF-Token': csrf,
        },
        body: JSON.stringify({ids}),
    });
    const text = await res.text();
    let data = null;
    try { data = JSON.parse(text); } catch (e) {}
    return {ok: res.ok, status: res.status, data};
}
"""

_ALGOLIA_FETCH_JS = """
async ({url, headers, body}) => {
    const res = await fetch(url, {method: 'POST', headers, body});
    const text = await res.text();
    let data = null;
    try { data = JSON.parse(text); } catch (e) {}
    return {ok: res.ok, status: res.status, data};
}
"""


def _company_id(value: Any) -> Any:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _is_companies_fetch(response: Any) -> bool:
    url = (getattr(response, "url", None) or "").split("?", 1)[0]
    method = ""
    try:
        method = (response.request.method or "").upper()
    except Exception:
        return False
    return method == "POST" and url.rstrip("/").endswith("/companies/fetch")


def _request_post_data(request: Any) -> str:
    try:
        return request.post_data or ""
    except Exception:
        return ""


def _is_waas_algolia_request(request: Any) -> bool:
    url = (getattr(request, "url", None) or "").lower()
    if not any(marker in url for marker in _ALGOLIA_HOST_MARKERS):
        return False
    method = (getattr(request, "method", None) or "").upper()
    if method != "POST":
        return False
    post = _request_post_data(request)
    return (
        "company_id" in post
        or "WaaS" in post
        or "CompanyJob" in post
    )


def _is_waas_algolia_search(response: Any) -> bool:
    try:
        return _is_waas_algolia_request(response.request)
    except Exception:
        return False


def _algolia_headers_from_request(request: Any) -> dict[str, str]:
    items: list[dict[str, str]] = []
    try:
        items = request.headers_array()
    except Exception:
        items = [
            {"name": str(k), "value": str(v)}
            for k, v in (getattr(request, "headers", None) or {}).items()
        ]
    out: dict[str, str] = {}
    for item in items:
        name = item.get("name") or ""
        if name.lower() in _ALGOLIA_HEADER_KEYS:
            out[name] = item.get("value") or ""
    return out


def _unwrap_algolia(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    results = payload.get("results")
    if isinstance(results, list) and results and isinstance(results[0], dict):
        return results[0]
    return payload


def _company_ids_from_hits(payload: Any) -> list[Any]:
    ids: list[Any] = []
    seen: set[Any] = set()
    for hit in _unwrap_algolia(payload).get("hits") or []:
        if not isinstance(hit, dict):
            continue
        cid = _company_id(hit.get("company_id"))
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        ids.append(cid)
    return ids


def _set_query_page(params: str, page: int) -> str:
    if re.search(r"(?:^|&)page=", params):
        return re.sub(r"(^|&)page=\d*", rf"\g<1>page={page}", params, count=1)
    sep = "&" if params else ""
    return f"{params}{sep}page={page}"


def _body_for_page(post_data: str, page: int) -> str:
    stripped = (post_data or "").lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            data = json.loads(post_data)
        except json.JSONDecodeError:
            return _set_query_page(post_data, page) if "page=" in post_data else post_data
        requests = data.get("requests")
        if not isinstance(requests, list):
            return json.dumps(data)
        for req in requests:
            if not isinstance(req, dict):
                continue
            params = req.get("params")
            if isinstance(params, str):
                req["params"] = _set_query_page(params, page)
            elif isinstance(params, dict):
                params["page"] = page
            else:
                req["page"] = page
        return json.dumps(data)
    if "page=" in (post_data or ""):
        return _set_query_page(post_data, page)
    return post_data


def _ingest_companies_payload(
    store: dict[Any, dict[str, Any]], data: Any
) -> list[dict[str, Any]]:
    batch: list[dict[str, Any]] = []
    if not isinstance(data, dict):
        return batch
    companies = data.get("companies")
    if not isinstance(companies, list):
        return batch
    for company in companies:
        if not isinstance(company, dict):
            continue
        cid = _company_id(company.get("id"))
        if cid is None:
            continue
        company = {**company, "id": cid}
        store[cid] = company
        batch.append(company)
    return batch


def _batch_is_stale(companies: list[dict[str, Any]], cutoff: datetime) -> bool:
    _jobs, dated = _jobs_from_companies(companies, cutoff)
    return bool(dated) and all(dt < cutoff for dt in dated)


def _wait_algolia_opts(page: Any) -> bool:
    try:
        page.wait_for_function(
            """() => {
                const o = window.AlgoliaOpts;
                return !!(o && o.app && o.key && !o.indices_not_set);
            }""",
            timeout=_NAV_TIMEOUT_MS,
        )
        return True
    except Exception:
        logger.warning("WAAS AlgoliaOpts not ready on the companies page")
        return False


def _wait_loading_idle(page: Any) -> None:
    loader = page.locator("text=Loading matches...")
    try:
        loader.first.wait_for(state="visible", timeout=400)
        loader.first.wait_for(state="hidden", timeout=_SCROLL_WAIT_MS)
    except Exception:
        pass


def _scroll_directory(page: Any) -> None:
    # InfiniteScroll uses the window (threshold 1000px), not an inner scroller.
    page.evaluate(
        """() => {
            const list = document.querySelector('.directory-list');
            if (list) {
                list.scrollIntoView({ block: 'end' });
            }
            window.scrollTo(0, document.body.scrollHeight);
            window.scrollBy(0, Math.max(window.innerHeight, 1200));
        }"""
    )


def _remember_algolia_request(request: Any, captured: dict[str, Any]) -> None:
    if captured.get("url") or not _is_waas_algolia_request(request):
        return
    captured["url"] = request.url
    captured["headers"] = _algolia_headers_from_request(request)
    captured["post_data"] = _request_post_data(request)
    logger.info("WAAS captured logged-in Algolia search request")


def _capture_algolia_response(response: Any, captured: dict[str, Any]) -> None:
    try:
        payload = response.json()
    except Exception:
        return
    result = _unwrap_algolia(payload)
    if not captured.get("url"):
        request = response.request
        captured["url"] = request.url
        captured["headers"] = _algolia_headers_from_request(request)
        captured["post_data"] = _request_post_data(request)
    if captured.get("first_payload") is not None:
        return
    captured["first_payload"] = payload
    captured["first_ids"] = _company_ids_from_hits(payload)
    captured["nb_pages"] = int(result.get("nbPages") or 1)
    captured["nb_hits"] = int(result.get("nbHits") or 0)
    logger.info(
        f"WAAS Algolia: {captured['nb_hits']} hits, {captured['nb_pages']} pages"
    )


def _algolia_search(page: Any, captured: dict[str, Any], page_num: int) -> dict[str, Any]:
    if page_num == 0 and isinstance(captured.get("first_payload"), dict):
        return captured["first_payload"]
    url = captured.get("url") or ""
    post_data = captured.get("post_data") or ""
    if not url or not post_data:
        return {}
    body = _body_for_page(post_data, page_num)
    try:
        result = page.evaluate(
            _ALGOLIA_FETCH_JS,
            {
                "url": url,
                "headers": captured.get("headers") or {},
                "body": body,
            },
        )
    except Exception as e:
        logger.warning(f"WAAS Algolia replay failed page={page_num}: {e}")
        return {}
    if not isinstance(result, dict):
        return {}
    if not result.get("ok"):
        logger.warning(
            f"WAAS Algolia HTTP {result.get('status')} page={page_num}"
        )
        return {}
    data = result.get("data")
    return data if isinstance(data, dict) else {}


def _companies_fetch(page: Any, ids: list[Any]) -> dict[str, Any]:
    if not ids:
        return {}
    try:
        result = page.evaluate(_COMPANIES_FETCH_JS, ids)
    except Exception as e:
        logger.warning(f"WAAS /companies/fetch failed: {e}")
        return {}
    if not isinstance(result, dict):
        return {}
    if not result.get("ok"):
        logger.warning(
            f"WAAS /companies/fetch HTTP {result.get('status')} "
            f"for {len(ids)} ids"
        )
        return {}
    data = result.get("data")
    return data if isinstance(data, dict) else {}


def _hydrate_ids(
    page: Any,
    store: dict[Any, dict[str, Any]],
    ids: list[Any],
) -> list[dict[str, Any]]:
    missing = [cid for cid in ids if cid not in store]
    if missing:
        batch = _ingest_companies_payload(store, _companies_fetch(page, missing))
        if batch:
            logger.info(
                f"WAAS companies/fetch: {len(batch)} companies "
                f"({len(store)} total)"
            )
    return [store[cid] for cid in ids if cid in store]


def _ingest_raw_companies(page: Any, store: dict[Any, dict[str, Any]]) -> None:
    try:
        html = page.content()
    except Exception:
        return
    props = _inertia_payload(html).get("props") or {}
    raw = props.get("rawCompanies") or []
    if isinstance(raw, list) and raw:
        _ingest_companies_payload(store, {"companies": raw})


def _collect_companies(page: Any, cutoff: datetime) -> list[dict[str, Any]]:
    store: dict[Any, dict[str, Any]] = {}
    captured: dict[str, Any] = {}

    def on_request(request: Any) -> None:
        try:
            _remember_algolia_request(request, captured)
        except Exception:
            return

    def on_response(response: Any) -> None:
        try:
            if _is_waas_algolia_search(response) and response.status == 200:
                _capture_algolia_response(response, captured)
            elif _is_companies_fetch(response) and response.status == 200:
                batch = _ingest_companies_payload(store, response.json())
                if batch:
                    logger.info(
                        f"WAAS companies/fetch: {len(batch)} companies "
                        f"({len(store)} total)"
                    )
        except Exception:
            return

    page.on("request", on_request)
    page.on("response", on_response)
    try:
        if _goto(page, WAAS_COMPANIES_URL, label="companies") is None:
            return []
        _wait_algolia_opts(page)
        try:
            page.wait_for_selector(".directory-list", timeout=15_000)
        except Exception:
            pass
        _wait_for_first_search(page, captured, store)
        if captured.get("first_payload") is None and not store:
            logger.info("WAAS reloading directory after AlgoliaOpts is ready")
            try:
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
            except Exception as e:
                logger.info(f"WAAS reload failed ({type(e).__name__})")
                return list(store.values())
            _wait_algolia_opts(page)
            _wait_for_first_search(page, captured, store)
        if not _on_companies_directory(page.url):
            logger.error(
                "WAAS login did not reach /companies "
                f"(url={page.url})"
            )
            return []
        _ingest_raw_companies(page, store)
        # Algolia → 250ms debounce → /companies/fetch
        page.wait_for_timeout(1_200)
        _wait_loading_idle(page)

        logger.info(
            f"WAAS directory ready: {len(store)} companies, "
            f"algolia_captured={bool(captured.get('url'))}"
        )
        if captured.get("url"):
            _collect_via_algolia_pages(page, store, captured, cutoff)
        else:
            logger.warning(
                "WAAS page did not issue an Algolia search — "
                f"falling back to infinite scroll url={page.url}"
            )
            _collect_via_scroll(page, store, cutoff)
    finally:
        try:
            page.remove_listener("request", on_request)
        except Exception:
            pass
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass

    if not store:
        logger.warning("WAAS directory loaded 0 companies")
    return list(store.values())


def _wait_for_first_search(
    page: Any,
    captured: dict[str, Any],
    store: dict[Any, dict[str, Any]],
) -> None:
    for _ in range(60):
        if captured.get("first_payload") is not None or store:
            return
        page.wait_for_timeout(250)


def _companies_for_ids(
    store: dict[Any, dict[str, Any]], ids: list[Any]
) -> list[dict[str, Any]]:
    return [store[cid] for cid in ids if cid in store]


def _collect_via_algolia_pages(
    page: Any,
    store: dict[Any, dict[str, Any]],
    captured: dict[str, Any],
    cutoff: datetime,
) -> None:
    if captured.get("first_payload") is None and captured.get("url"):
        payload = _algolia_search(page, captured, 0)
        if isinstance(payload, dict) and payload:
            captured["first_payload"] = payload
            captured["first_ids"] = _company_ids_from_hits(payload)
            result = _unwrap_algolia(payload)
            captured["nb_pages"] = int(result.get("nbPages") or 1)
            captured["nb_hits"] = int(result.get("nbHits") or 0)
            logger.info(
                f"WAAS Algolia: {captured['nb_hits']} hits, "
                f"{captured['nb_pages']} pages"
            )

    nb_pages = min(int(captured.get("nb_pages") or 1), _MAX_PAGES)
    if captured.get("nb_hits") == 0:
        logger.warning("WAAS Algolia search returned 0 company hits")
        first_ids = captured.get("first_ids") or []
        if first_ids:
            _hydrate_ids(page, store, first_ids)
        return

    for page_idx in range(nb_pages):
        payload = _algolia_search(page, captured, page_idx)
        ids = _company_ids_from_hits(payload)
        logger.info(
            f"WAAS companies page {page_idx + 1}/{nb_pages}: {len(ids)} ids"
        )
        if not ids:
            break
        batch = _hydrate_ids(page, store, ids)
        stale_from = batch or _companies_for_ids(store, ids)
        if _batch_is_stale(stale_from, cutoff):
            logger.info(
                f"WAAS company page {page_idx + 1} is fully stale — stopping"
            )
            break
        if page_idx + 1 < nb_pages:
            time.sleep(_FETCH_DELAY)


def _collect_via_scroll(
    page: Any,
    store: dict[Any, dict[str, Any]],
    cutoff: datetime,
) -> None:
    logger.info(
        f"WAAS infinite-scroll fallback ({len(store)} companies so far)"
    )
    idle_rounds = 0
    last_count = len(store)
    for scroll in range(_MAX_SCROLLS):
        logger.info(
            f"WAAS scroll {scroll + 1}/{_MAX_SCROLLS} companies={len(store)}"
        )
        _wait_loading_idle(page)
        snapshot = list(store.values())
        if snapshot and _batch_is_stale(snapshot[-_HITS_PER_PAGE:], cutoff):
            logger.info(
                f"WAAS scrolled batch is fully stale after {scroll} scrolls"
            )
            break
        try:
            with page.expect_response(
                _is_companies_fetch, timeout=_SCROLL_WAIT_MS
            ):
                _scroll_directory(page)
        except Exception:
            _scroll_directory(page)
            page.wait_for_timeout(1_000)
        _wait_loading_idle(page)
        if len(store) == last_count:
            idle_rounds += 1
            if idle_rounds >= 3:
                logger.info(
                    f"WAAS directory idle after {scroll + 1} scrolls "
                    f"({len(store)} companies)"
                )
                break
        else:
            idle_rounds = 0
            last_count = len(store)


def _enrich_details(page: Any, jobs: list[dict[str, Any]], on_job=None) -> None:
    total = len(jobs)
    for i, job in enumerate(jobs):
        try:
            html = _page_html(page, job["url"])
            _merge_detail(job, html)
        except Exception as e:
            logger.info(f"WAAS detail skip ({type(e).__name__}) for {job['url']}")
            logger.debug(traceback.format_exc())
        if on_job:
            on_job(job)
        done = i + 1
        if done == total or done % 25 == 0:
            logger.info(f"WAAS details {done}/{total}")
        if done < total:
            time.sleep(_FETCH_DELAY)


def _inertia_payload(html: str) -> dict[str, Any]:
    match = _DATA_PAGE_RE.search(html or "")
    if not match:
        return {}
    try:
        data = json.loads(html_lib.unescape(match.group(1)))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


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


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    text = str(value).strip()
    if text.isdigit():
        return _parse_dt(int(text))
    rel = _parse_relative_date(text)
    if rel:
        return rel
    try:
        dt = dateutil_parser.parse(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _skill_label(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("name") or value.get("label") or "").strip()
    if isinstance(value, str):
        text = value.strip()
        if "jobs_skill" in text:
            return sanitize_skill_object_dumps(text)
        return text
    return ""


def _listing_description(item: dict[str, Any]) -> str:
    chunks: list[str] = []
    one_liner = (item.get("companyOneLiner") or item.get("one_liner") or "").strip()
    if one_liner:
        chunks.append(one_liner)
    salary = (
        item.get("salaryRange")
        or item.get("pretty_salary_range")
        or item.get("salary")
        or ""
    )
    salary = str(salary).strip()
    if salary:
        chunks.append(f"Salary: {salary}")
    equity = (
        item.get("equityRange")
        or item.get("pretty_equity_range")
        or ""
    )
    equity = str(equity).strip()
    if equity:
        chunks.append(f"Equity: {equity}")
    skills = item.get("skills")
    if isinstance(skills, list):
        names = [_skill_label(s) for s in skills]
        names = [n for n in names if n]
        if names:
            chunks.append("Skills: " + ", ".join(names))
    return "\n".join(chunks)


def _merge_detail(job: dict[str, Any], html: str) -> None:
    props = _inertia_payload(html).get("props") or {}
    detail = props.get("job")
    if not isinstance(detail, dict):
        return
    company_blob = props.get("company") if isinstance(props.get("company"), dict) else {}
    description = (detail.get("description") or "").strip()
    listing_src = dict(detail)
    if company_blob.get("description") and not listing_src.get("companyOneLiner") and not listing_src.get("one_liner"):
        listing_src["one_liner"] = str(company_blob.get("description") or "")
    extra = _listing_description(listing_src)
    html_body = detail.get("descriptionHtml") if isinstance(detail.get("descriptionHtml"), str) else ""
    html_body = html_body.strip()
    interview = detail.get("interviewProcessHtml") if isinstance(detail.get("interviewProcessHtml"), str) else ""
    interview = interview.strip()
    body = description or html_body
    if interview:
        body = f"{body}\n\nInterview process\n{interview}" if body else interview
    if body:
        job["description"] = body if not extra else f"{body}\n\n{extra}"
    loc = _location_text(detail.get("location")) or ""
    if loc:
        job["location"] = loc
    company = (detail.get("companyName") or company_blob.get("name") or "").strip()
    if company:
        job["company"] = company
    title = (detail.get("title") or "").strip()
    if title:
        job["title"] = title
    created = _parse_dt(detail.get("createdAt") or detail.get("created_at"))
    if created:
        job["posted_date"] = created


def hydrate_job_from_public_page(url: str) -> str | None:
    """Load descriptionHtml from the public WAAS job page."""
    if not url or not _is_waas_host(url):
        return None
    try:
        resp = requests.get(url, headers=_PUBLIC_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"WAAS public hydrate failed for {url}: {e}")
        return None
    parsed: dict[str, Any] = {"url": url, "description": ""}
    _merge_detail(parsed, resp.text)
    text = (parsed.get("description") or "").strip()
    return text or None


def _jobs_from_companies(
    companies: list[Any],
    cutoff: datetime,
) -> tuple[list[dict[str, Any]], list[datetime]]:
    jobs: list[dict[str, Any]] = []
    dated: list[datetime] = []
    for company in companies:
        if not isinstance(company, dict):
            continue
        raw_jobs = company.get("jobs")
        if not isinstance(raw_jobs, list):
            continue
        for item in raw_jobs:
            if not isinstance(item, dict):
                continue
            parsed = _parse_waas_job(company, item)
            if not parsed:
                continue
            posted = parsed.get("posted_date")
            if posted:
                dated.append(posted)
                if posted < cutoff:
                    continue
            jobs.append(parsed)
    return jobs, dated


def _parse_waas_job(
    company: dict[str, Any],
    item: dict[str, Any],
) -> dict[str, Any] | None:
    global _LOGGED_RAW_JOB_KEYS
    if not _LOGGED_RAW_JOB_KEYS:
        _LOGGED_RAW_JOB_KEYS = True
        logger.info(f"WAAS raw job keys: {sorted(item.keys())}")
    title = (item.get("title") or "").strip()
    if not title or not _is_engineering_title(title):
        return None
    role = str(item.get("role") or "").strip().lower()
    if role and role != "eng":
        return None
    url = _waas_job_url(company, item)
    if not url:
        return None
    job_id = str(item.get("id") or url)
    location = (
        _location_text(item.get("pretty_location_or_remote"))
        or _location_text(item.get("location"))
        or "Remote"
    )
    posted = _parse_dt(
        item.get("created_at")
        or item.get("createdAt")
        or item.get("posted_at")
        or item.get("postedAt")
    )
    company_name = (
        (company.get("name") or item.get("companyName") or "").strip()
        or "Unknown"
    )
    return {
        "id": job_id,
        "title": title,
        "company": company_name,
        "url": url,
        "description": _listing_description({**company, **item}),
        "location": location,
        "posted_date": posted,
        "last_active": str(item.get("lastActive") or item.get("last_active") or ""),
    }


def _waas_job_url(company: dict[str, Any], item: dict[str, Any]) -> str:
    path = (
        item.get("show_path")
        or item.get("showPath")
        or item.get("url")
        or ""
    )
    path = str(path).strip()
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if path.startswith("/companies/"):
        return urljoin(YC_BASE, path)
    if path.startswith("/"):
        host = WAAS_BASE if path.startswith("/jobs") else YC_BASE
        return urljoin(host, path)
    slug = (company.get("slug") or item.get("companySlug") or "").strip()
    job_id = item.get("id")
    if slug and job_id:
        return f"{YC_BASE}/companies/{slug}/jobs/{job_id}"
    if job_id:
        return f"{WAAS_BASE}/jobs/{job_id}"
    return ""


def _launch_browser(pw: Any) -> Any:
    args = ["--disable-http2"]
    try:
        return pw.chromium.launch(headless=True, channel="chrome", args=args)
    except Exception:
        return pw.chromium.launch(headless=True, args=args)


def _login(page: Any, email: str, password: str) -> bool:
    continue_to = quote(WAAS_COMPANIES_URL, safe="")
    login_url = f"{ACCOUNT_LOGIN_URL}?continue={continue_to}"
    if _goto(page, login_url, label="login") is None:
        return False
    if _is_waas_host(page.url):
        return True
    if page.locator("iframe[src*='recaptcha'], iframe[src*='hcaptcha'], iframe[src*='turnstile']").count():
        logger.error("WAAS login requires captcha — cannot continue headless")
        halt(
            "login_halt",
            "WAAS login requires captcha. Sign in manually, then rerun the fetch.",
            source="waas",
        )
    if page.locator("#ycid-input").count() == 0:
        logger.error("WAAS login form not found")
        return False

    page.fill("#ycid-input", email)
    if page.locator("#password-input").count() == 0:
        page.locator("button[type=submit]").first.click()
        try:
            page.wait_for_selector("#password-input", timeout=15_000)
        except Exception:
            pw_btn = page.get_by_role("button", name=re.compile(r"password", re.I))
            if pw_btn.count():
                pw_btn.first.click()
                page.wait_for_selector("#password-input", timeout=15_000)

    if page.locator("#password-input").count() == 0:
        logger.error("WAAS password field not found (magic link / extra challenge?)")
        return False

    page.fill("#password-input", password)
    page.locator("button[type=submit]").first.click()

    for _ in range(40):
        if _is_waas_host(page.url):
            return True
        if page.locator("input[name='totp'], #totp-input, input[autocomplete='one-time-code']").count():
            logger.error("WAAS login requires TOTP — cannot continue")
            halt(
                "login_halt",
                "WAAS login requires a one-time code. Finish sign-in manually, then rerun the fetch.",
                source="waas",
            )
        page.wait_for_timeout(500)

    logger.error("WAAS login did not succeed")
    return False


@contextmanager
def _browser_session(email: str, password: str):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = _launch_browser(pw)
        context = None
        try:
            context = browser.new_context(
                user_agent=_UA,
                viewport={"width": 1280, "height": 720},
            )
            page = context.new_page()
            if not _login(page, email, password):
                yield None
                return
            yield page
        finally:
            if context is not None:
                context.close()
            browser.close()


def _page_html(page: Any, url: str) -> str:
    resp = _goto(page, url, label="detail")
    if resp is None:
        return ""
    if getattr(resp, "status", 200) >= 400:
        logger.info(f"WAAS HTTP {resp.status} on {url}")
        return ""
    return page.content()
