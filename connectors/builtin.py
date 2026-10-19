"""
BuiltIn connector.

Guest listing HTML at ``/jobs/remote/{category}`` for engineering,
AI/ML, data-analytics, and program/project-management (parent
categories only — stacking parents keeps the last one). ``requests``
gets SSR ``job-card`` markup; robots disallows ``/apply/``,
``*?search=``, and ``/jobs/*mid-level``. Guest GraphQL
``customFilteredJobs`` has no ``daysSinceUpdated`` / subcategory
filters.

``daysSinceUpdated`` maps ``max_job_age_days`` onto BuiltIn's 1/3/7/30
radios. No ``q=``. Seniority is ``job_inclusion``. Dates on a page are
mixed — walk ``?page=`` per category (runaway cap). Dedup by job id.
Engineering title filter (same keywords as RemoteYeah). Skip known
URLs. Skip detail HTTP; the card has title, location, blurb, and ISO
``published_date`` in the tracking payload.

Guest job HTML has empty ``applyUrl`` and a Login-to-apply CTA.
``requests`` never hits ``/apply/`` (robots). CloakBrowser
(``CLOAKBROWSER_USER_DATA``, default ``.cloak-builtin``) is used only
for the final link. Optional ``CLOAKBROWSER_PROXY``
(``http://user:pass@host:port`` or ``socks5://…``) is that browser's
exit. It opens ``/job/…``, uses off-site ``jobPostInit.applyUrl`` when
present, otherwise follows ``?handler=ApplyRedirect`` via the
``Location`` header (ATS page is not loaded), or clicks Apply, and keeps
the landing URL only if it leaves builtin.com. The first signed-out
job page waits until that page reports the account signed in.
Apply capture does not continue while the login is unfinished.
Easy Apply / login wall keep the listing URL. Never persist
``builtin.com/apply/…``.

``location`` is a string.
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
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from dateutil import parser as dateutil_parser
from dotenv import load_dotenv

from connectors.base import BaseConnector
from utils.alerts import ScrapeHalted, alert, halt
from utils.ats_detector import detect_ats
from utils.job_age import job_age_cutoff, max_job_age_days
from utils.job_inclusion import exclusion_reason, load_candidate_profile
from utils.job_store import (
    remember_listing_urls,
    source_job_urls,
    stored_seen_listings,
    unseen_listing_urls,
    update_job_url,
)
from utils.logger import setup_logger
from utils.text_cleaning import clean_description

load_dotenv()

logger = setup_logger("builtin_connector")

BASE_URL = "https://builtin.com"
# Parent categories only. Stacking these slugs binds the last parent, not a union.
LISTING_PATHS = (
    "/jobs/remote/engineering",
    "/jobs/remote/ai-machine-learning",
    "/jobs/remote/data-analytics",
    "/jobs/remote/program-project-management",
)
LISTING_PATH = LISTING_PATHS[0]
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": f"{BASE_URL}/jobs/remote",
}
_FETCH_DELAY = 0.5
_RETRY_DELAY = 1.5
_RETRIES = 3
_MAX_CONSECUTIVE_FAILURES = 3
# Mixed-date pager; 3-day window was 8 pages of 10. Runaway only.
_MAX_PAGES = 40
_LISTING_TIMEOUT = 40
_NAV_TIMEOUT_MS = 30_000
_APPLY_WAIT_MS = 15_000
_APPLY_GAP_S = 2.0
_SECURITY_WAIT_S = 30
_LOGIN_WAIT_S = 600
_SECURITY_MARKERS = (
    "performing security verification",
    "just a moment",
    "cf-browser-verification",
    "checking your browser",
)
_AGE_WINDOWS = (1, 3, 7, 30)
_LOGIN_APPLY_SELECTOR = (
    'a[aria-label="Login to apply"], '
    'a[href*="/auth/login"][data-builtin-track-click-event="job_apply_unauth"]'
)
_APPLY_CLICK_SELECTORS = (
    'a[href*="/apply/"]',
    '[data-builtin-track-click-event="job_apply"]'
    ':not([data-builtin-track-login-button="1"])',
    'a[aria-label="Apply"]',
    'button:has-text("Apply Now")',
    'a:has-text("Apply Now")',
    'button:has-text("Apply")',
    'a.btn:has-text("Apply")',
)
_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "fullstack", "devops", "sre", "data engineer",
    "data analyst", "data scientist", "machine learning", "ml ", " ml",
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
_ARRANGEMENT_PHRASES = (
    "in-office or remote",
    "remote or hybrid",
    "fully remote",
    "on site",
    "on-site",
    "hybrid",
    "remote",
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_CARD_SPLIT_RE = re.compile(r'<div id="job-card-', re.I)
_CARD_ID_RE = re.compile(r'^(\d+)"')
_TITLE_RE = re.compile(
    r'<a href="(/job/[^"]+)"[^>]*data-id="job-card-title"[^>]*>(.*?)</a>',
    re.I | re.S,
)
_COMPANY_RE = re.compile(
    r'data-id="company-title"[^>]*>.*?<span>([^<]+)</span>',
    re.I | re.S,
)
_HOUSE_RE = re.compile(
    r'fa-house-building[^>]*>.*?<span class="font-barlow text-gray-04">'
    r"([^<]+)</span>",
    re.I | re.S,
)
_PLACE_RE = re.compile(
    r'fa-location-dot[^>]*>.*?<span class="font-barlow text-gray-04">'
    r"([^<]+)</span>",
    re.I | re.S,
)
_MULTI_PLACE_RE = re.compile(
    r'data-bs-title="([^"]*)"[^>]*>\s*\d+\s+Locations',
    re.I,
)
_DESC_RE = re.compile(
    r'<div class="fs-sm fw-regular mb-md text-gray-04">(.*?)</div>',
    re.I | re.S,
)
_CLOCK_RE = re.compile(
    r"fa-clock[^>]*>\s*</i>\s*((?:Reposted\s+)?(?:An?\s+\w+\s+Ago|"
    r"Yesterday|\d+\s+\w+\s+Ago))",
    re.I,
)
_RELATIVE_RE = re.compile(
    r"(?:Reposted\s+)?(?:"
    r"(?P<a>An?)\s+(?P<aunit>Minute|Hour)\s+Ago|"
    r"(?P<n>\d+)\s+(?P<unit>Minutes?|Hours?|Days?)\s+Ago|"
    r"(?P<yday>Yesterday)"
    r")",
    re.I,
)
_PUBLISHED_RE = re.compile(
    r"'id'\s*:\s*(\d+)\s*,\s*'published_date'\s*:\s*'([^']+)'"
)
_PAGE_RE = re.compile(r"[?&]page=(\d+)", re.I)
_NEXT_RE = re.compile(r'aria-label="Go to Next Page"', re.I)


def _cloak_user_data_dir() -> str:
    load_dotenv()
    raw = os.getenv("CLOAKBROWSER_USER_DATA", "").strip().strip('"').strip("'")
    if raw:
        return os.path.normpath(raw)
    return os.path.normpath(
        os.path.join(os.path.dirname(os.path.dirname(__file__)), ".cloak-builtin")
    )


def _cloak_headless() -> bool:
    return os.getenv("CLOAKBROWSER_HEADLESS", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _cloak_proxy() -> str:
    load_dotenv()
    return os.getenv("CLOAKBROWSER_PROXY", "").strip().strip('"').strip("'")


def _proxy_for_log(proxy: str) -> str:
    """Host and port only, so credentials never land in the log."""
    raw = proxy if "://" in proxy else f"http://{proxy}"
    parsed = urlparse(raw)
    if not parsed.hostname:
        return "set"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def _host(url: str) -> str:
    host = urlparse(url or "").netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _strip_utm(url: str) -> str:
    parsed = urlparse(url)
    kept = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() != "ref"
    ]
    return urlunparse(parsed._replace(query=urlencode(kept)))


def _offsite_apply_url(apply_url: Any) -> str:
    url = _strip_utm(str(apply_url or "").strip())
    if not url or _is_noise_url(url):
        return ""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return ""
    host = _host(url)
    if not host or host == "builtin.com" or host.endswith(".builtin.com"):
        return ""
    return url


_NOISE_HOSTS = (
    "osano.com",
    "google-analytics.com",
    "googletagmanager.com",
    "googleadservices.com",
    "doubleclick.net",
    "gstatic.com",
    "fontawesome.com",
    "googleapis.com",
)
_ASSET_SUFFIXES = (
    ".js", ".css", ".woff", ".woff2", ".ttf",
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
)


def _is_noise_url(url: str) -> bool:
    """Trackers and asset files are not the employer apply page."""
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower()
    if any(path.endswith(suffix) for suffix in _ASSET_SUFFIXES):
        return True
    host = _host(url)
    return any(host == noise or host.endswith(f".{noise}") for noise in _NOISE_HOSTS)


def _job_post_init(html: str) -> dict[str, Any]:
    match = re.search(r"jobPostInit\((\{)", html or "")
    if not match:
        return {}
    try:
        data, _ = json.JSONDecoder().raw_decode(html[match.start(1):])
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _apply_url_from_init(data: dict[str, Any]) -> str:
    job = data.get("job") if isinstance(data.get("job"), dict) else {}
    if job.get("isEasyApply"):
        return ""
    return _offsite_apply_url(job.get("applyUrl"))


def _builtin_apply_redirect(apply_url: Any) -> str:
    """Return the /job/…?handler=ApplyRedirect URL when that is BuiltIn's apply CTA."""
    raw = str(apply_url or "").strip()
    if not raw:
        return ""
    url = raw if "://" in raw else urljoin(f"{BASE_URL}/", raw)
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return ""
    host = _host(url)
    if host != "builtin.com" and not host.endswith(".builtin.com"):
        return ""
    if not parsed.path.startswith("/job/"):
        return ""
    params = {
        k.lower(): v
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
    }
    if params.get("handler", "").lower() != "applyredirect":
        return ""
    return url


def _listing_for_job(external_id: str, stored_url: str, seen: list[str]) -> str:
    host = _host(stored_url)
    if host == "builtin.com" or host.endswith(".builtin.com"):
        return stored_url
    suffix = f"/{external_id}"
    for listing in seen:
        path = urlparse(listing).path.rstrip("/")
        listing_host = _host(listing)
        if path.endswith(suffix) and (
            listing_host == "builtin.com" or listing_host.endswith(".builtin.com")
        ):
            return listing
    return ""


def _apply_recapture_targets(source: str) -> list[tuple[str, str]]:
    """Listing URLs whose stored apply URL is still missing or was a tracker."""
    seen = stored_seen_listings(source)
    targets: list[tuple[str, str]] = []
    seen_ids: set[str] = set()
    for external_id, stored_url in source_job_urls(source):
        if _offsite_apply_url(stored_url):
            continue
        listing = _listing_for_job(external_id, stored_url, seen)
        if not listing or external_id in seen_ids:
            continue
        seen_ids.add(external_id)
        targets.append((external_id, listing))
    return targets


def _follow_apply_redirect_headers(page: Any, start_url: str) -> str:
    """Save the employer document URL once navigation commits.

    ``wait_until="commit"`` returns when the browser has the final address
    and has started the response. The page body is not waited on, and
    subresources are not inspected. A catch-all route is not used: its
    ``continue_()`` call sits on the same thread as the navigation and can
    stall the run for good when the proxy does not answer.
    """
    if not start_url:
        return ""
    try:
        page.goto(start_url, wait_until="commit", timeout=_NAV_TIMEOUT_MS)
    except Exception as e:
        logger.info(f"builtin ApplyRedirect navigation failed: {e}")
    return _offsite_apply_url(getattr(page, "url", "") or "")


class _ApplySession:
    def __init__(self):
        self.resolved = 0
        self._cm: Any = None
        self._pair: tuple[Any, Any] | None = None
        self._failed = False
        self._login_ok = False
        self._login_blocked = False

    def launched(self) -> bool:
        return self._pair is not None

    def resolve(self, job: dict[str, Any]) -> str:
        listing = job.get("listing_url") or ""
        if not listing:
            return ""
        pair = self._ensure_browser()
        if pair is None:
            return ""
        self.resolved += 1
        context, page = pair
        if self._login_blocked:
            halt(
                "login_halt",
                "builtin.com is signed out. The scrape cannot store the employer apply URL.",
                source="builtin",
            )
        if not self._login_ok:
            try:
                signed_in = _ensure_builtin_login(
                    page, listing, reveal=self._reveal_for_login
                )
            except ScrapeHalted:
                raise
            except Exception as e:
                logger.info(
                    f"builtin login wait failed for {job.get('id')}: {e}"
                )
                signed_in = False
            if not signed_in:
                halt(
                    "login_halt",
                    "builtin.com sign-in was not finished within "
                    f"{_LOGIN_WAIT_S // 60} minutes.",
                    source="builtin",
                )
            self._login_ok = True
            context, page = self._pair or (context, page)
        try:
            found = _resolve_apply_on_page(context, page, listing)
        except ScrapeHalted:
            raise
        except Exception as e:
            logger.info(
                f"builtin apply capture failed for {job.get('id')}: {e}"
            )
            found = ""
        time.sleep(_APPLY_GAP_S)
        if found:
            logger.info(f"builtin apply URL {job.get('id')}: {found}")
        else:
            logger.info(
                f"builtin apply capture kept listing for {job.get('id')}"
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
            halt(
                "browser",
                f"builtin CloakBrowser profile failed to launch: {e}",
                source="builtin",
            )
        return self._pair

    def _reveal_for_login(self) -> Any:
        """Show the window when sign-in is required during a hidden run."""
        if not _cloak_headless():
            return self._pair[1] if self._pair else None
        logger.info("builtin showing CloakBrowser so you can sign in")
        self.close()
        self._cm = _browser_session(headless=False)
        self._pair = self._cm.__enter__()
        return self._pair[1]

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
def _browser_session(headless: bool | None = None):
    from cloakbrowser import launch_persistent_context

    user_data = _cloak_user_data_dir()
    if headless is None:
        headless = _cloak_headless()
    proxy = _cloak_proxy()
    context = launch_persistent_context(
        user_data,
        headless=headless,
        locale="en-US",
        humanize=True,
        **({"proxy": proxy} if proxy else {}),
    )
    try:
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.set_default_navigation_timeout(_LISTING_TIMEOUT * 1000)
        except Exception:
            pass
        logger.info(
            "builtin apply capture using CloakBrowser "
            f"user-data-dir={user_data} headless={headless} "
            f"proxy={_proxy_for_log(proxy) if proxy else 'direct'}"
        )
        yield context, page
    finally:
        context.close()


def _apply_locator(page: Any) -> Any | None:
    if page.locator(_LOGIN_APPLY_SELECTOR).count() and not page.locator(
        'a[href*="/apply/"]'
    ).count():
        return None
    for selector in _APPLY_CLICK_SELECTORS:
        loc = page.locator(selector)
        if loc.count():
            return loc.first
    return None


def _first_offsite_url(context: Any, page: Any) -> str:
    pages = list(getattr(context, "pages", None) or [page])
    for candidate in pages:
        url = _offsite_apply_url(getattr(candidate, "url", "") or "")
        if url:
            return url
    return ""


def _close_extra_pages(context: Any, page: Any) -> None:
    for extra in list(getattr(context, "pages", None) or []):
        if extra is page:
            continue
        try:
            extra.close()
        except Exception:
            pass


def _wait_offsite(context: Any, page: Any) -> str:
    deadline = time.time() + (_APPLY_WAIT_MS / 1000)
    while time.time() < deadline:
        found = _first_offsite_url(context, page)
        if found:
            _close_extra_pages(context, page)
            return found
        try:
            page.wait_for_timeout(250)
        except Exception:
            time.sleep(0.25)
    found = _first_offsite_url(context, page)
    _close_extra_pages(context, page)
    return found


def _is_security_check(html: str) -> bool:
    low = (html or "").lower()
    return any(marker in low for marker in _SECURITY_MARKERS)


def _page_html(page: Any) -> str:
    try:
        return page.content() or ""
    except Exception:
        return ""


def _wait_past_security_check(page: Any, listing_url: str) -> str:
    """Stay on Cloudflare's automatic check until the job page is back.

    The interstitial clears in a real Chrome window if we do not navigate
    away. Leaving it and opening the next job restarts the check.
    """
    html = _page_html(page)
    if "jobPostInit" in html or not _is_security_check(html):
        return html
    logger.info(f"builtin security check — waiting ({listing_url})")
    for reload in (False, True):
        if reload:
            logger.info("builtin security check still up — reloading once")
            try:
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
            except Exception:
                pass
        deadline = time.time() + _SECURITY_WAIT_S
        while time.time() < deadline:
            html = _page_html(page)
            if "jobPostInit" in html or not _is_security_check(html):
                logger.info(f"builtin security check cleared ({listing_url})")
                return html
            try:
                page.wait_for_timeout(1000)
            except Exception:
                time.sleep(1)
    logger.info(f"builtin security check did not clear ({listing_url})")
    halt(
        "security_check",
        "builtin robot check did not clear "
        f"({listing_url}). This browser or IP looks flagged. "
        "Later job pages will keep showing the check. Fix that, then rerun the fetch.",
        source="builtin",
    )


def _logged_in_flag(html: str) -> bool | None:
    init = _job_post_init(html)
    if "isLoggedIn" not in init:
        return None
    return bool(init.get("isLoggedIn"))


def _on_builtin_login_page(url: str) -> bool:
    return "/auth/login" in (url or "") or "/auth/" in (url or "")


def _ensure_builtin_login(page: Any, listing_url: str, *, reveal=None) -> bool:
    """Wait for a BuiltIn sign-in before Apply capture.

    A missing ``isLoggedIn`` flag is not success. The login form often has no
    flag at all, and treating that as signed-in sent the scraper to the next job.
    The login page is left alone. Reading its HTML or fetching the job URL
    from it trips the content-security policy and disturbs the reCAPTCHA
    check that BuiltIn runs before it sends the email link.

    A signed-out profile pauses here for ``_LOGIN_WAIT_S`` so the
    administrator can sign in. A hidden window is shown first when
    ``reveal`` does that. The scrape stops only after that wait expires.
    """
    current = getattr(page, "url", "") or ""
    if listing_url not in current:
        try:
            page.goto(
                listing_url,
                wait_until="domcontentloaded",
                timeout=_NAV_TIMEOUT_MS,
            )
        except Exception:
            pass
    flag = _logged_in_flag(_page_html(page))
    if flag is True:
        return True
    if flag is not False:
        return True
    minutes = _LOGIN_WAIT_S // 60
    alert(
        "action",
        "login_wait",
        "builtin.com is signed out. Sign in in the CloakBrowser window "
        f"within {minutes} minutes. The scrape is waiting.",
        source="builtin",
    )
    if reveal is not None:
        shown = reveal()
        if shown is not None:
            page = shown
    logger.info(
        "builtin.com is signed out. Sign in in the CloakBrowser window. "
        f"Waiting up to {minutes} minutes."
    )
    try:
        page.goto(
            f"{BASE_URL}/auth/login",
            wait_until="domcontentloaded",
            timeout=_NAV_TIMEOUT_MS,
        )
    except Exception:
        pass
    deadline = time.time() + _LOGIN_WAIT_S
    while time.time() < deadline:
        try:
            page.wait_for_timeout(1000)
        except Exception:
            time.sleep(1)
        current = getattr(page, "url", "") or ""
        if _on_builtin_login_page(current):
            continue
        if listing_url not in current:
            try:
                page.goto(
                    listing_url,
                    wait_until="domcontentloaded",
                    timeout=_NAV_TIMEOUT_MS,
                )
            except Exception:
                pass
        if _logged_in_flag(_page_html(page)) is True:
            logger.info("builtin.com login saved in the CloakBrowser profile")
            return True
        if not _on_builtin_login_page(getattr(page, "url", "") or ""):
            try:
                page.goto(
                    f"{BASE_URL}/auth/login",
                    wait_until="domcontentloaded",
                    timeout=_NAV_TIMEOUT_MS,
                )
            except Exception:
                pass
    logger.warning(
        "builtin.com login was not finished — skipping Apply capture "
        "for the rest of this run"
    )
    halt(
        "login_halt",
        "builtin.com sign-in was not finished within "
        f"{_LOGIN_WAIT_S // 60} minutes. The scrape stopped.",
        source="builtin",
    )


def _resolve_apply_on_page(
    context: Any,
    page: Any,
    listing_url: str,
    wait_for_login: bool = False,
) -> str:
    page.goto(listing_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
    html = _wait_past_security_check(page, listing_url)
    if wait_for_login and not _ensure_builtin_login(page, listing_url):
        return ""
    if wait_for_login:
        html = _page_html(page)
    if "jobPostInit" not in html:
        try:
            page.wait_for_function(
                "() => document.documentElement.innerHTML.includes('jobPostInit')",
                timeout=8_000,
            )
        except Exception:
            pass
        html = _page_html(page)
    if _is_security_check(html) and "jobPostInit" not in html:
        return ""
    init = _job_post_init(html)
    from_init = _apply_url_from_init(init)
    if from_init:
        return from_init
    job = init.get("job") if isinstance(init.get("job"), dict) else {}
    if job.get("isEasyApply"):
        logger.info(f"builtin Easy Apply — keeping listing URL ({listing_url})")
        return ""
    redirect = _builtin_apply_redirect(job.get("applyUrl"))
    if redirect:
        found = _follow_apply_redirect_headers(page, redirect)
        if found:
            return found
        try:
            page.goto(redirect, wait_until="commit", timeout=_NAV_TIMEOUT_MS)
        except Exception:
            pass
        found = (
            _offsite_apply_url(getattr(page, "url", "") or "")
            or _first_offsite_url(context, page)
        )
        if not found:
            logger.info(
                f"builtin ApplyRedirect stayed on builtin.com ({listing_url})"
            )
        return found
    locator = _apply_locator(page)
    if locator is None:
        logger.info(f"builtin no Apply control ({listing_url})")
        return ""
    try:
        with context.expect_page(timeout=3_000) as popup_info:
            locator.click(timeout=5_000)
        popup = popup_info.value
        try:
            popup.wait_for_load_state("domcontentloaded", timeout=_APPLY_WAIT_MS)
        except Exception:
            pass
    except Exception:
        pass
    found = _wait_offsite(context, page)
    if not found:
        logger.info(f"builtin Apply click stayed on builtin.com ({listing_url})")
    return found


class BuiltinConnector(BaseConnector):
    def __init__(self):
        self.source_name = "builtin"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        age_days = max_job_age_days(self.source_name)
        cutoff = job_age_cutoff(self.source_name)
        window = days_since_updated(age_days)
        logger.info(
            "Fetching jobs from builtin.com listing HTML "
            f"(remote engineering/AI/data/PM categories, "
            f"daysSinceUpdated={window}, country=USA, "
            f"age_days={age_days}; mixed-date walk)…"
        )
        kept: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        apply_session = _ApplySession()
        try:
            self._recapture_stored_apply_urls(apply_session)
            for path in LISTING_PATHS:
                consecutive_failures = 0
                for page in range(1, _MAX_PAGES + 1):
                    html = _fetch_listing(page, window, path)
                    if html is None:
                        consecutive_failures += 1
                        logger.warning(
                            f"builtin {path} page {page} failed "
                            f"({consecutive_failures}/{_MAX_CONSECUTIVE_FAILURES}) — "
                            "keeping prior jobs, continuing"
                        )
                        if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                            break
                        time.sleep(_RETRY_DELAY)
                        continue
                    consecutive_failures = 0
                    if not html:
                        break
                    published = _published_by_id(html)
                    cards = _iter_cards(html)
                    page_jobs: list[dict[str, Any]] = []
                    for job_id, card in cards:
                        raw = _parse_card(job_id, card, published)
                        if not raw:
                            continue
                        posted = raw.get("posted_date")
                        if posted is not None and posted < cutoff:
                            continue
                        if not _is_engineering_title(raw["title"]):
                            continue
                        if raw["id"] in seen_ids:
                            continue
                        seen_ids.add(raw["id"])
                        page_jobs.append(raw)
                    logger.info(
                        f"builtin {path} page {page}: {len(cards)} cards, "
                        f"{len(page_jobs)} engineering"
                    )
                    self._emit_page(page_jobs, kept, apply_session)
                    if not cards or not _has_next(html, page):
                        break
                    if page < _MAX_PAGES:
                        time.sleep(_FETCH_DELAY)
        except ScrapeHalted:
            raise
        except Exception as e:
            logger.error(f"Error fetching jobs from BuiltIn: {e}")
            logger.debug(traceback.format_exc())
        finally:
            if apply_session.launched() and apply_session.resolved == 0:
                logger.info(
                    "builtin CloakBrowser had no Apply clicks this run "
                    "(no unseen eligible listings and no stored builtin.com URLs)"
                )
            apply_session.close()
        logger.info(f"Successfully fetched {len(kept)} jobs from builtin")
        return kept

    def _recapture_stored_apply_urls(self, apply_session: "_ApplySession") -> None:
        targets = _apply_recapture_targets(self.source_name)
        if not targets:
            logger.info("builtin no stored listing URLs needing apply capture")
            return
        logger.info(
            f"builtin recapturing Apply URLs for {len(targets)} stored jobs"
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
            logger.info(f"builtin updated {updated} stored jobs with off-site Apply URLs")

    def _emit_page(
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
                f"builtin {len(page_jobs)} engineering listings already seen — "
                "skipping apply capture on this page"
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
                f"builtin skipped {skipped} ineligible listings before persist"
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


def days_since_updated(age_days: int) -> int:
    """Map pipeline age days onto BuiltIn's 1/3/7/30 radios (next window up)."""
    for option in _AGE_WINDOWS:
        if option >= age_days:
            return option
    return _AGE_WINDOWS[-1]


def listing_url(page: int, window: int, path: str | None = None) -> str:
    params = {
        "daysSinceUpdated": window,
        "city": "",
        "state": "",
        "country": "USA",
        "allLocations": "true",
    }
    if page > 1:
        params["page"] = page
    return f"{BASE_URL}{path or LISTING_PATH}?{urlencode(params)}"


def _is_engineering_title(title: str) -> bool:
    return any(kw in title.lower() for kw in _ENGINEERING_KEYWORDS)


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


def _parse_relative(text: str, now: datetime | None = None) -> datetime | None:
    if not text:
        return None
    match = _RELATIVE_RE.search(text)
    if not match:
        return None
    now = now or datetime.now(timezone.utc)
    if match.group("yday"):
        return now - timedelta(days=1)
    if match.group("a"):
        unit = (match.group("aunit") or "").lower()
        if unit.startswith("minute"):
            return now - timedelta(minutes=1)
        if unit.startswith("hour"):
            return now - timedelta(hours=1)
        return now
    n = int(match.group("n") or 0)
    unit = (match.group("unit") or "").lower()
    if unit.startswith("minute"):
        return now - timedelta(minutes=n)
    if unit.startswith("hour"):
        return now - timedelta(hours=n)
    if unit.startswith("day"):
        return now - timedelta(days=n)
    return now


def _published_by_id(html: str) -> dict[str, datetime]:
    found: dict[str, datetime] = {}
    for job_id, raw in _PUBLISHED_RE.findall(html):
        dt = _parse_dt(raw)
        if dt:
            found[str(job_id)] = dt
    return found


def _iter_cards(html: str) -> list[tuple[str, str]]:
    cards: list[tuple[str, str]] = []
    for part in _CARD_SPLIT_RE.split(html)[1:]:
        match = _CARD_ID_RE.match(part)
        if not match:
            continue
        cards.append((match.group(1), part))
    return cards


def _max_page(html: str, current: int) -> int:
    nums = [int(n) for n in _PAGE_RE.findall(html)]
    return max([current, *nums], default=current)


def _has_next(html: str, page: int) -> bool:
    if _NEXT_RE.search(html):
        return True
    return _max_page(html, page) > page


def _arrangement(card: str) -> str:
    match = _HOUSE_RE.search(card)
    if match:
        return _plain(match.group(1))
    lowered = card.lower()
    for phrase in _ARRANGEMENT_PHRASES:
        if phrase in lowered:
            return phrase
    return ""


def _places(card: str) -> list[str]:
    places: list[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        label = _plain(raw)
        if not label or re.search(r"\d+\s+Locations?", label, re.I):
            return
        if re.search(r"\d+\s*K|Annually|Hourly", label, re.I):
            return
        key = label.lower()
        if key in seen:
            return
        seen.add(key)
        places.append(label)

    match = _PLACE_RE.search(card)
    if match:
        _add(match.group(1))
    multi = _MULTI_PLACE_RE.search(card)
    if multi:
        decoded = html_lib.unescape(multi.group(1))
        for chunk in re.findall(r">([^<]+)<", decoded) or [_plain(decoded)]:
            _add(chunk)
    return places


def _location_text(card: str) -> str:
    arrangement = _arrangement(card)
    places = _places(card)
    place_str = "; ".join(places)
    if arrangement and place_str:
        return f"{arrangement}, {place_str}"
    return arrangement or place_str or "Remote"


def _inclusion_fields(job: dict[str, Any]) -> dict[str, Any]:
    location = job.get("location") or "Remote"
    return {
        "title": job.get("title") or "",
        "location": location,
        "raw_location_text": location,
        "description": job.get("description") or "",
        "description_text": job.get("description") or "",
    }


def _parse_card(
    job_id: str,
    card: str,
    published: dict[str, datetime],
) -> dict[str, Any] | None:
    title_m = _TITLE_RE.search(card)
    if not title_m:
        return None
    path = html_lib.unescape(title_m.group(1)).strip()
    title = _plain(title_m.group(2))
    if not title or not path:
        return None
    listing_url = urljoin(BASE_URL, path)
    company_m = _COMPANY_RE.search(card)
    company = _plain(company_m.group(1)) if company_m else "Unknown"
    desc_m = _DESC_RE.search(card)
    description = _plain(desc_m.group(1)) if desc_m else ""
    posted = published.get(job_id)
    if posted is None:
        clock = _CLOCK_RE.search(card)
        posted = _parse_relative(clock.group(1) if clock else "")
    return {
        "id": job_id,
        "listing_url": listing_url,
        "url": listing_url,
        "title": title,
        "company": company or "Unknown",
        "location": _location_text(card),
        "description": description,
        "posted_date": posted,
    }


def _fetch_listing(page: int, window: int, path: str) -> str | None:
    """Return listing HTML from HTTP, or None after retries."""
    url = listing_url(page, window, path)
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_LISTING_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.info(
                f"builtin GET failed ({type(e).__name__}) "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After") or "60"
            logger.info(f"builtin GET HTTP 429 Retry-After={retry_after}")
            if attempt < _RETRIES:
                try:
                    wait = float(retry_after)
                except ValueError:
                    wait = _RETRY_DELAY * attempt
                time.sleep(min(wait, 60))
            continue
        body = resp.text or ""
        if _is_security_check(body) and "job-card-" not in body:
            logger.info(
                f"builtin listing security check "
                f"attempt {attempt}/{_RETRIES}"
            )
            halt(
                "security_check",
                "builtin listing hit a robot check "
                f"({url}). This browser or IP looks flagged. "
                "Fix that, then rerun the fetch.",
                source="builtin",
            )
        if resp.status_code >= 400:
            logger.info(
                f"builtin GET HTTP {resp.status_code} "
                f"attempt {attempt}/{_RETRIES}"
            )
            if attempt < _RETRIES:
                time.sleep(_RETRY_DELAY * attempt)
            continue
        return resp.text or ""
    return None
