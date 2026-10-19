"""
Mocked tests for BuiltinConnector.

Covers: listing URL (daysSinceUpdated, no search/seniority), card parse,
location as a string, engineering title filter, mixed-date pager walk
(no first-stale stop), skip ineligible before persist, no detail HTTP,
CloakBrowser apply-URL capture (off-site only), and normalize() shape.
No live browser.
"""
from __future__ import annotations

import html as html_lib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from utils.alerts import ScrapeHalted


def _clock_past(limit: int):
    """Advance a few timestamps, then jump past a wait deadline.

    Logging also reads the clock. A clock that freezes below the deadline
    makes ``time.sleep`` wait forever.
    """
    state = {"n": 0}

    def _now():
        state["n"] += 1
        if state["n"] >= 8:
            return limit + 10_000
        return state["n"]

    return _now

from connectors.builtin import (
    BASE_URL,
    LISTING_PATH,
    LISTING_PATHS,
    BuiltinConnector,
    _APPLY_CLICK_SELECTORS,
    _LOGIN_APPLY_SELECTOR,
    _apply_locator,
    _apply_url_from_init,
    _builtin_apply_redirect,
    _follow_apply_redirect_headers,
    _is_engineering_title,
    _job_post_init,
    _location_text,
    _offsite_apply_url,
    _parse_card,
    _resolve_apply_on_page,
    days_since_updated,
    listing_url,
)


_NOW = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
_CUTOFF = _NOW - timedelta(days=10)


def _card(
    job_id="11268876",
    slug="senior-machine-learning-engineer",
    title="Senior Machine Learning Engineer",
    company="Acme",
    arrangement="Remote or Hybrid",
    place="California, USA",
    extra_places=None,
    age="2 Days Ago",
    description="Python PyTorch production ML role",
):
    extra = ""
    if extra_places:
        inner = "".join(
            f"<div class='text-truncate'>{html_lib.escape(p)}</div>"
            for p in extra_places
        )
        extra = (
            f'<span data-bs-title="{html_lib.escape(inner)}">2 Locations</span>'
        )
    return f"""
<div id="job-card-{job_id}" data-id="job-card">
  <a href="/company/acme" data-id="company-title"><span>{company}</span></a>
  <a href="/job/{slug}/{job_id}" target="_blank" data-id="job-card-title">{title}</a>
  <i class="fa-regular fa-clock"></i>{age}</span>
  <i class="fa-regular fa-house-building"></i>
  <span class="font-barlow text-gray-04">{arrangement}</span>
  <i class="fa-regular fa-location-dot"></i>
  <span class="font-barlow text-gray-04">{place}</span>
  {extra}
  <div class="fs-sm fw-regular mb-md text-gray-04">{description}</div>
</div>
"""


def _page(cards, *, page=1, has_next=False, published=None):
    pager = ""
    if has_next:
        nxt = page + 1
        pager = f"""
<div id="pagination">
  <a href="{LISTING_PATH}?daysSinceUpdated=3&amp;page={nxt}" aria-label="Go to Page {nxt}">{nxt}</a>
  <a href="{LISTING_PATH}?daysSinceUpdated=3&amp;page={nxt}" aria-label="Go to Next Page">next</a>
</div>
"""
    jobs_js = ""
    if published:
        rows = ",".join(
            f"{{'id':{jid},'published_date':'{dt}','featured':false}}"
            for jid, dt in published
        )
        jobs_js = (
            "<script>window.bix.eventTracking.logBuiltinTrackEvent("
            f"'job_board_view', {{'jobs':[{rows}]}});</script>"
        )
    return f"<html><body>{''.join(cards)}{pager}{jobs_js}</body></html>"


class _Resp:
    def __init__(self, text="", status=200, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.text = text


def test_days_since_updated_maps_pipeline_age():
    assert days_since_updated(1) == 1
    assert days_since_updated(2) == 3
    assert days_since_updated(3) == 3
    assert days_since_updated(7) == 7
    assert days_since_updated(10) == 30


def test_proxy_log_hides_credentials():
    from connectors.builtin import _proxy_for_log

    assert _proxy_for_log("http://user:secret@proxy.example:8080") == (
        "http://proxy.example:8080"
    )
    assert _proxy_for_log("socks5://user:secret@10.1.2.3:1080") == (
        "socks5://10.1.2.3:1080"
    )
    assert "secret" not in _proxy_for_log("user:secret@proxy.example:8080")


def test_cloak_session_passes_proxy(monkeypatch):
    from connectors.builtin import _browser_session

    monkeypatch.setenv("CLOAKBROWSER_PROXY", "http://user:secret@proxy.example:8080")
    monkeypatch.setenv("CLOAKBROWSER_HEADLESS", "0")
    page = MagicMock()
    context = MagicMock()
    context.pages = [page]
    with patch("cloakbrowser.launch_persistent_context", return_value=context) as launch:
        with _browser_session():
            pass
    assert launch.call_args.kwargs["proxy"] == "http://user:secret@proxy.example:8080"
    assert launch.call_args.kwargs["headless"] is False
    assert "stealth_args" not in launch.call_args.kwargs
    assert "chromium_sandbox" not in launch.call_args.kwargs
    assert "args" not in launch.call_args.kwargs


def test_cloak_session_omits_proxy_when_unset(monkeypatch):
    from connectors.builtin import _browser_session

    monkeypatch.setenv("CLOAKBROWSER_PROXY", "")
    page = MagicMock()
    context = MagicMock()
    context.pages = [page]
    with patch("cloakbrowser.launch_persistent_context", return_value=context) as launch:
        with _browser_session():
            pass
    assert "proxy" not in launch.call_args.kwargs
    assert "args" not in launch.call_args.kwargs


def test_listing_url_has_no_search_or_seniority():
    url = listing_url(1, 3)
    assert url.startswith(f"{BASE_URL}{LISTING_PATH}?")
    assert LISTING_PATHS == (
        "/jobs/remote/engineering",
        "/jobs/remote/ai-machine-learning",
        "/jobs/remote/data-analytics",
        "/jobs/remote/program-project-management",
    )
    assert all(
        listing_url(1, 3, path).startswith(f"{BASE_URL}{path}?")
        for path in LISTING_PATHS
    )
    assert "daysSinceUpdated=3" in url
    assert "country=USA" in url
    assert "allLocations=true" in url
    assert "search=" not in url
    assert "mid-level" not in url
    assert "senior" not in url
    assert "page=" not in url
    assert "page=2" in listing_url(2, 3)


def test_parse_card_location_is_string():
    job = _parse_card(
        "11268876",
        _card(),
        {"11268876": datetime(2026, 9, 19, 22, 58, 36, tzinfo=timezone.utc)},
    )
    assert job is not None
    assert job["id"] == "11268876"
    assert job["title"] == "Senior Machine Learning Engineer"
    assert job["company"] == "Acme"
    assert job["location"] == "Remote or Hybrid, California, USA"
    assert isinstance(job["location"], str)
    assert job["listing_url"] == (
        "https://builtin.com/job/senior-machine-learning-engineer/11268876"
    )
    assert job["posted_date"] == datetime(
        2026, 9, 19, 22, 58, 36, tzinfo=timezone.utc
    )
    assert "PyTorch" in job["description"]


def test_location_joins_arrangement_place_and_tooltip():
    text = _location_text(_card(
        arrangement="In-Office or Remote",
        place="Tempe, AZ, USA",
        extra_places=["CA, USA", "Santa Clara, CA, USA"],
    ))
    assert text.startswith("In-Office or Remote, Tempe, AZ, USA")
    assert "CA, USA" in text
    assert "Santa Clara, CA, USA" in text
    assert isinstance(text, str)


def test_engineering_title_filter():
    eng = _parse_card("1", _card(), {})
    sales = _parse_card("2", _card(
        job_id="2",
        title="Account Executive",
        slug="account-executive",
    ), {})
    assert _is_engineering_title(eng["title"])
    assert _is_engineering_title("Sr. Data Analyst - Finance")
    assert not _is_engineering_title(sales["title"])
    assert not _is_engineering_title("Space Systems Program Manager")


class _EmptyLocator:
    def count(self):
        return 0

    @property
    def first(self):
        return self

    def click(self, timeout=0):
        return None


class _ListingNav:
    def __init__(self, status, url):
        self.status = status
        self.url = url
        self.headers = {}


class _ListingBrowser:
    """Fake CloakBrowser page. ``handler(url)`` returns HTML or ``(status, html)``."""

    def __init__(self, handler):
        self._handler = handler
        self.url = "about:blank"
        self._html = ""
        self.visits: list[str] = []

    def goto(self, url, **kwargs):
        self.visits.append(url)
        self.url = url
        outcome = self._handler(url)
        if isinstance(outcome, tuple):
            status, html = outcome
        else:
            status, html = 200, outcome
        self._html = html or ""
        return _ListingNav(status, url)

    def content(self):
        return self._html

    def locator(self, selector):
        return _EmptyLocator()

    def wait_for_function(self, *args, **kwargs):
        return None

    def wait_for_timeout(self, ms):
        return None

    def reload(self, **kwargs):
        return None


class _ListingContext:
    def __init__(self, page):
        self.pages = [page]
        self.request = None


@contextmanager
def _cloak_fetch(handler):
    page = _ListingBrowser(handler)
    ctx = _ListingContext(page)

    @contextmanager
    def _session():
        yield ctx, page

    with patch("connectors.builtin._browser_session", _session):
        yield page


def _listing_visits(page: _ListingBrowser) -> list[str]:
    return [url for url in page.visits if "/jobs/" in url]


@patch("connectors.builtin.remember_listing_urls")
@patch("connectors.builtin.unseen_listing_urls", side_effect=lambda urls, source: list(urls))
@patch("connectors.builtin.time.sleep")
@patch("connectors.builtin.exclusion_reason", return_value=None)
@patch(
    "connectors.builtin.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.builtin.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.builtin.max_job_age_days", return_value=2)
@patch("connectors.builtin.stored_seen_listings", return_value=[])
@patch("connectors.builtin.source_job_urls", return_value=[])
@patch("connectors.builtin.requests.get")
def test_fetch_keeps_engineering_walks_mixed_dates(mock_get, *_patches):
    page1 = _page(
        [
            _card(),
            _card(
                job_id="2",
                title="Account Executive",
                slug="account-executive",
                description="Sales quota",
            ),
            _card(
                job_id="3",
                title="Staff Software Engineer",
                slug="old-staff-engineer",
            ),
        ],
        page=1,
        has_next=True,
        published=[
            ("11268876", "2026-09-19T22:58:36"),
            ("2", "2026-09-19T12:00:00"),
            ("3", "2026-08-01T12:00:00"),
        ],
    )
    page2 = _page(
        [
            _card(
                job_id="4",
                title="Platform Engineer",
                slug="platform-engineer",
                description="Kubernetes Python",
            ),
        ],
        page=2,
        has_next=False,
        published=[("4", "2026-09-18T12:00:00")],
    )

    def _get(url, **kwargs):
        assert "daysSinceUpdated=3" in url
        assert "search=" not in url
        assert "mid-level" not in url
        if "/jobs/remote/engineering" not in url:
            return _Resp(_page([], has_next=False))
        if "page=2" in url:
            return _Resp(page2)
        if "page=" in url:
            raise AssertionError(f"unexpected listings page {url}")
        return _Resp(page1)

    mock_get.side_effect = _get
    with _cloak_fetch(lambda url: ""):
        jobs = BuiltinConnector().fetch_jobs()
    ids = [j["id"] for j in jobs]
    assert ids == ["11268876", "4"]
    assert jobs[0]["url"].startswith("https://builtin.com/job/")
    fetched = [c.args[0] for c in mock_get.call_args_list if c.args]
    assert listing_url(1, 3) in fetched
    assert listing_url(2, 3) in fetched
    assert all(
        listing_url(1, 3, path) in fetched for path in LISTING_PATHS
    )
    assert len(fetched) == 2 + (len(LISTING_PATHS) - 1)


@patch("connectors.builtin.remember_listing_urls")
@patch("connectors.builtin.unseen_listing_urls", side_effect=lambda urls, source: list(urls))
@patch("connectors.builtin.time.sleep")
@patch(
    "connectors.builtin.exclusion_reason",
    return_value=("remote", "Location not eligible: In-Office or Remote, Tempe, AZ, USA"),
)
@patch(
    "connectors.builtin.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.builtin.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.builtin.max_job_age_days", return_value=2)
@patch("connectors.builtin.stored_seen_listings", return_value=[])
@patch("connectors.builtin.source_job_urls", return_value=[])
@patch("connectors.builtin.requests.get")
def test_skips_ineligible_without_storing(mock_get, *_patches):
    mock_get.return_value = _Resp(_page([_card(
        arrangement="In-Office or Remote",
        place="Tempe, AZ, USA",
    )], published=[("11268876", "2026-09-19T22:58:36")]))
    with _cloak_fetch(lambda url: ""):
        jobs = BuiltinConnector().fetch_jobs()
    assert jobs == []


@patch("connectors.builtin.remember_listing_urls")
@patch("connectors.builtin.unseen_listing_urls", side_effect=lambda urls, source: list(urls))
@patch("connectors.builtin.time.sleep")
@patch("connectors.builtin.exclusion_reason", return_value=None)
@patch(
    "connectors.builtin.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.builtin.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.builtin.max_job_age_days", return_value=2)
@patch("connectors.builtin.stored_seen_listings", return_value=[])
@patch("connectors.builtin.source_job_urls", return_value=[])
@patch("connectors.builtin.requests.get")
def test_no_detail_http(mock_get, *_patches):
    mock_get.return_value = _Resp(_page(
        [_card()],
        published=[("11268876", "2026-09-19T22:58:36")],
    ))
    with _cloak_fetch(lambda url: "") as browser:
        jobs = BuiltinConnector().fetch_jobs()
    assert len(jobs) == 1
    urls = [c.args[0] for c in mock_get.call_args_list if c.args]
    assert urls == [listing_url(1, 3, path) for path in LISTING_PATHS]
    assert all("/apply/" not in u and "/job/" not in u for u in urls)
    assert browser.visits
    assert all("/jobs/" not in u for u in browser.visits)


@patch("connectors.builtin.remember_listing_urls")
@patch("connectors.builtin.unseen_listing_urls", side_effect=lambda urls, source: list(urls))
@patch("connectors.builtin.time.sleep")
@patch("connectors.builtin.exclusion_reason", return_value=None)
@patch(
    "connectors.builtin.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.builtin.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.builtin.max_job_age_days", return_value=2)
@patch("connectors.builtin.stored_seen_listings", return_value=[])
@patch("connectors.builtin.source_job_urls", return_value=[])
@patch("connectors.builtin.requests.get")
def test_timeout_mid_walk_keeps_prior_and_continues(mock_get, *_patches):
    import requests

    page1 = _page(
        [_card()],
        page=1,
        has_next=True,
        published=[("11268876", "2026-09-19T22:58:36")],
    )
    page3 = _page(
        [
            _card(
                job_id="4",
                title="Platform Engineer",
                slug="platform-engineer",
                description="Kubernetes Python",
            ),
        ],
        page=3,
        has_next=False,
        published=[("4", "2026-09-18T12:00:00")],
    )

    def _get(url, **kwargs):
        if "/jobs/remote/engineering" not in url:
            return _Resp(_page([], has_next=False))
        if "page=3" in url:
            return _Resp(page3)
        if "page=2" in url:
            raise requests.ConnectionError("reset")
        return _Resp(page1)

    mock_get.side_effect = _get
    with _cloak_fetch(lambda url: ""):
        jobs = BuiltinConnector().fetch_jobs()
    ids = [j["id"] for j in jobs]
    assert "11268876" in ids
    assert "4" in ids
    # engineering: page1 + page2 three retries + page3; other cats: page1
    assert mock_get.call_count == 5 + (len(LISTING_PATHS) - 1)


class TestNormalize:
    def _raw(self):
        return {
            "id": "11268876",
            "listing_url": "https://builtin.com/job/senior-machine-learning-engineer/11268876",
            "url": "https://builtin.com/job/senior-machine-learning-engineer/11268876",
            "title": "Senior Machine Learning Engineer",
            "company": "Acme",
            "location": "Remote or Hybrid, California, USA",
            "description": "Python role",
            "posted_date": datetime(2026, 9, 19, tzinfo=timezone.utc),
        }

    def test_shape(self):
        from tests.connectors.test_normalize import _assert_shape
        n = BuiltinConnector().normalize(self._raw())
        _assert_shape(n, "builtin")

    def test_keeps_builtin_url_and_string_location(self):
        n = BuiltinConnector().normalize(self._raw())
        assert n["url"].startswith("https://builtin.com/job/")
        assert n["location"] == "Remote or Hybrid, California, USA"
        assert isinstance(n["location"], str)

    def test_coerces_non_string_location(self):
        raw = self._raw()
        raw["location"] = [{"city": "California"}]
        n = BuiltinConnector().normalize(raw)
        assert n["location"] == "Remote"
        assert isinstance(n["location"], str)

    def test_keeps_offsite_apply_url(self):
        raw = self._raw()
        raw["url"] = "https://boards.greenhouse.io/acme/jobs/1"
        n = BuiltinConnector().normalize(raw)
        assert n["url"] == "https://boards.greenhouse.io/acme/jobs/1"


def test_offsite_apply_url_rejects_builtin_and_keeps_ats():
    assert _offsite_apply_url("") == ""
    assert _offsite_apply_url("https://builtin.com/apply/11612906") == ""
    assert _offsite_apply_url("https://www.builtin.com/job/x/1") == ""
    assert _offsite_apply_url(
        "https://boards.greenhouse.io/acme/jobs/1?utm_source=builtin"
    ) == "https://boards.greenhouse.io/acme/jobs/1"


def test_job_post_init_reads_apply_url_and_skips_easy_apply():
    html = (
        'jobPostInit({"job":{"id":1,"applyUrl":'
        '"https://jobs.ashbyhq.com/acme/abc","isEasyApply":false},"isLoggedIn":true});'
    )
    data = _job_post_init(html)
    assert _apply_url_from_init(data) == "https://jobs.ashbyhq.com/acme/abc"
    easy = _job_post_init(
        'jobPostInit({"job":{"applyUrl":"https://jobs.ashbyhq.com/x","isEasyApply":true}});'
    )
    assert _apply_url_from_init(easy) == ""
    empty = _job_post_init(
        'jobPostInit({"job":{"applyUrl":"","isEasyApply":false},"isLoggedIn":false});'
    )
    assert _apply_url_from_init(empty) == ""
    relative = _job_post_init(
        'jobPostInit({"job":{"applyUrl":'
        '"/job/x/1?handler=ApplyRedirect","isEasyApply":false}});'
    )
    assert _apply_url_from_init(relative) == ""
    assert _builtin_apply_redirect(
        "/job/x/1?handler=ApplyRedirect"
    ) == "https://builtin.com/job/x/1?handler=ApplyRedirect"
    assert _builtin_apply_redirect("https://jobs.ashbyhq.com/x") == ""


class _FakeLocator:
    def __init__(self, n=0, on_click=None):
        self._n = n
        self._on_click = on_click
        self.clicked = False

    def count(self):
        return self._n

    @property
    def first(self):
        return self

    def click(self, timeout=0):
        self.clicked = True
        if self._on_click:
            self._on_click()


class _FakePage:
    def __init__(self, html, url, locators):
        self._html = html
        self.url = url
        self._locators = locators

    def goto(self, url, **kwargs):
        self.url = url

    def wait_for_function(self, *args, **kwargs):
        return None

    def content(self):
        return self._html

    def locator(self, selector):
        return self._locators.get(selector, _FakeLocator(0))

    def wait_for_timeout(self, ms):
        return None

    def reload(self, **kwargs):
        return None


class _FakePopup:
    def __init__(self, url):
        self.url = url

    def wait_for_load_state(self, *args, **kwargs):
        return None

    def close(self):
        return None


class _FakeResponse:
    def __init__(self, status, location=None, url=""):
        self.status = status
        self.url = url
        self.headers = {"location": location} if location else {}


class _FakeAPIRequest:
    def __init__(self, hops=None):
        self.hops = hops or {}
        self.fetched = []

    def get(self, url, **kwargs):
        self.fetched.append(url)
        status, location = self.hops.get(url, (200, None))
        return _FakeResponse(status, location, url)


class _FakeContext:
    def __init__(self, pages, popup=None, redirect_hops=None):
        self.pages = pages
        self._popup = popup
        self.request = _FakeAPIRequest(redirect_hops)

    def expect_page(self, timeout=0):
        popup = self._popup

        @contextmanager
        def _cm():
            if popup is None:
                info = MagicMock()
                info.value = None
                yield info
                raise TimeoutError("no popup")
            info = MagicMock()
            info.value = popup
            self.pages.append(popup)
            yield info

        return _cm()


def test_security_check_waits_then_reads_apply_url():
    class _ClearingPage(_FakePage):
        def __init__(self):
            super().__init__(
                "<html>Performing security verification</html>",
                "https://builtin.com/job/x/1",
                {},
            )
            self.reads = 0

        def content(self):
            self.reads += 1
            if self.reads >= 3:
                return (
                    'jobPostInit({"job":{"applyUrl":'
                    '"https://jobs.ashbyhq.com/acme/1","isEasyApply":false}});'
                )
            return "<html>Performing security verification</html>"

    page = _ClearingPage()
    url = _resolve_apply_on_page(_FakeContext([page]), page, "https://builtin.com/job/x/1")
    assert url == "https://jobs.ashbyhq.com/acme/1"


def test_security_check_that_stays_up_stops_the_scrape():
    page = _FakePage(
        "<html>Just a moment</html>",
        "https://builtin.com/job/x/1",
        {},
    )
    ticks = iter([0, 0, 31, 31, 62, 100])
    with patch("connectors.builtin.time.time", side_effect=lambda: next(ticks, 100)):
        with pytest.raises(ScrapeHalted):
            _resolve_apply_on_page(_FakeContext([page]), page, "https://builtin.com/job/x/1")


def test_resolve_easy_apply_keeps_listing():
    html = 'jobPostInit({"job":{"applyUrl":"","isEasyApply":true}});'
    apply_btn = _FakeLocator(1)
    page = _FakePage(html, "https://builtin.com/job/x/1", {
        _APPLY_CLICK_SELECTORS[0]: apply_btn,
    })
    context = _FakeContext([page])
    assert _resolve_apply_on_page(context, page, "https://builtin.com/job/x/1") == ""
    assert not apply_btn.clicked


class _RedirectPage(_FakePage):
    """Browser page whose navigation lands on the final redirect URL."""

    def __init__(self, hops, html="", url="https://builtin.com/job/x/1", side_requests=None):
        super().__init__(html, url, {})
        self.hops = hops
        self.side_requests = side_requests or []

    def goto(self, url, **kwargs):
        current = url
        for _ in range(8):
            self.url = current
            _status, location = self.hops.get(current, (200, None))
            if not location:
                return
            current = location
        self.url = current


def test_follow_apply_redirect_ignores_tracker_requests():
    start = "https://builtin.com/job/x/1?handler=ApplyRedirect"
    ats = "https://careers.rocket.com/us/en/job/99"
    page = _RedirectPage(
        {start: (302, ats)},
        side_requests=[
            ("https://www.google-analytics.com/g/collect?v=2", "ping"),
            ("https://cmp.osano.com/", "script"),
            ("https://maps.gstatic.com/maps-api-v3/embed/js/init_embed.js", "script"),
        ],
    )
    assert _follow_apply_redirect_headers(page, start) == ats
    assert page.url == ats
    assert "google-analytics.com" not in page.url


@patch("connectors.builtin.stored_seen_listings", return_value=[
    "https://builtin.com/job/staff-software-developer/11736145",
])
@patch("connectors.builtin.source_job_urls", return_value=[
    ("11736145", "https://www.google-analytics.com/g/collect?v=2"),
    ("11704605", "https://job-boards.greenhouse.io/newrelic/jobs/5453390008"),
    ("11731884", "https://builtin.com/job/manager/11731884"),
])
def test_recapture_includes_tracker_urls_and_skips_real_apply_urls(_urls, _seen):
    from connectors.builtin import _apply_recapture_targets

    assert _apply_recapture_targets("builtin") == [
        ("11736145", "https://builtin.com/job/staff-software-developer/11736145"),
        ("11731884", "https://builtin.com/job/manager/11731884"),
    ]


def test_follow_apply_redirect_headers_stops_at_offsite_location():
    start = "https://builtin.com/job/x/1?handler=ApplyRedirect"
    apply_hop = "https://builtin.com/apply/1"
    ats = "https://jobs.ashbyhq.com/acme/abc"
    page = _RedirectPage({
        start: (302, apply_hop),
        apply_hop: (302, ats),
    })
    assert _follow_apply_redirect_headers(page, start) == ats
    assert page.url == ats


def test_resolve_follows_apply_redirect():
    html = (
        'jobPostInit({"job":{"applyUrl":'
        '"/job/x/1?handler=ApplyRedirect","isEasyApply":false}});'
    )
    start = "https://builtin.com/job/x/1?handler=ApplyRedirect"
    page = _RedirectPage(
        {start: (302, "https://jobs.ashbyhq.com/acme/abc")},
        html=html,
    )
    url = _resolve_apply_on_page(_FakeContext([page]), page, "https://builtin.com/job/x/1")
    assert url == "https://jobs.ashbyhq.com/acme/abc"
    assert page.url == "https://jobs.ashbyhq.com/acme/abc"


def test_resolve_uses_job_post_init_without_click():
    html = (
        'jobPostInit({"job":{"applyUrl":'
        '"https://boards.greenhouse.io/acme/jobs/99","isEasyApply":false}});'
    )
    apply_btn = _FakeLocator(1)
    page = _FakePage(html, "https://builtin.com/job/x/1", {
        'a[href*="/apply/"]': apply_btn,
    })
    context = _FakeContext([page])
    url = _resolve_apply_on_page(context, page, "https://builtin.com/job/x/1")
    assert url == "https://boards.greenhouse.io/acme/jobs/99"
    assert not apply_btn.clicked


def test_resolve_click_popup_captures_offsite():
    html = 'jobPostInit({"job":{"applyUrl":"","isEasyApply":false}});'
    popup = _FakePopup("https://jobs.lever.co/acme/abc")
    apply_btn = _FakeLocator(1)
    page = _FakePage(html, "https://builtin.com/job/x/1", {
        _LOGIN_APPLY_SELECTOR: _FakeLocator(0),
        _APPLY_CLICK_SELECTORS[0]: apply_btn,
    })
    context = _FakeContext([page], popup=popup)
    url = _resolve_apply_on_page(context, page, "https://builtin.com/job/x/1")
    assert url == "https://jobs.lever.co/acme/abc"
    assert apply_btn.clicked


def test_resolve_login_cta_keeps_listing():
    html = 'jobPostInit({"job":{"applyUrl":"","isEasyApply":false},"isLoggedIn":false});'
    page = _FakePage(html, "https://builtin.com/job/x/1", {
        _LOGIN_APPLY_SELECTOR: _FakeLocator(1),
        "a[href*=\"/apply/\"]": _FakeLocator(0),
    })
    context = _FakeContext([page])
    assert _resolve_apply_on_page(context, page, "https://builtin.com/job/x/1") == ""
    assert _apply_locator(page) is None


def test_login_wait_continues_after_sign_in(monkeypatch):
    monkeypatch.setenv("CLOAKBROWSER_HEADLESS", "0")
    signed_out = (
        'jobPostInit({"job":{"applyUrl":"","isEasyApply":false},"isLoggedIn":false});'
    )
    signed_in = (
        'jobPostInit({"job":{"applyUrl":'
        '"https://boards.greenhouse.io/acme/jobs/9","isEasyApply":false},'
        '"isLoggedIn":true});'
    )

    class _SigningIn(_FakePage):
        def __init__(self):
            super().__init__(signed_out, "https://builtin.com/job/x/1", {})
            self._signed_in = False
            self._waits = 0
            self.login_reads = 0

        def wait_for_timeout(self, ms):
            self._waits += 1
            if self._waits >= 2 and "/auth/login" in (self.url or ""):
                self._signed_in = True
                self.url = "https://builtin.com/"

        def content(self):
            if "/auth/login" in (self.url or ""):
                self.login_reads += 1
                return "<html><h1>Sign in</h1></html>"
            if self._signed_in:
                return signed_in
            return signed_out

    page = _SigningIn()
    clock = {"t": 0}

    def _now():
        clock["t"] += 1
        return clock["t"]

    with patch("connectors.builtin.time.time", side_effect=_now):
        url = _resolve_apply_on_page(
            _FakeContext([page]),
            page,
            "https://builtin.com/job/x/1",
            wait_for_login=True,
        )
    assert url == "https://boards.greenhouse.io/acme/jobs/9"
    assert page.url == "https://builtin.com/job/x/1"
    assert page.login_reads == 0


def test_login_page_without_signed_in_flag_does_not_continue(monkeypatch):
    monkeypatch.setenv("CLOAKBROWSER_HEADLESS", "0")
    from connectors.builtin import _LOGIN_WAIT_S

    signed_out = (
        'jobPostInit({"job":{"applyUrl":"","isEasyApply":false},"isLoggedIn":false});'
    )

    class _StaysOut(_FakePage):
        def content(self):
            if "/auth/login" in (self.url or ""):
                return "<html><h1>Sign in</h1></html>"
            return signed_out

    page = _StaysOut(signed_out, "https://builtin.com/job/x/1", {})
    with patch("connectors.builtin.time.time", side_effect=_clock_past(_LOGIN_WAIT_S)):
        with pytest.raises(ScrapeHalted):
            _resolve_apply_on_page(
                _FakeContext([page]),
                page,
                "https://builtin.com/job/x/1",
                wait_for_login=True,
            )


def test_session_skips_later_jobs_when_login_is_not_finished(monkeypatch):
    monkeypatch.setenv("CLOAKBROWSER_HEADLESS", "0")
    from connectors.builtin import _ApplySession, _LOGIN_WAIT_S

    signed_out = (
        'jobPostInit({"job":{"applyUrl":"","isEasyApply":false},"isLoggedIn":false});'
    )
    first = "https://builtin.com/job/x/1"
    second = "https://builtin.com/job/y/2"

    class _StaysOut(_FakePage):
        def __init__(self):
            super().__init__(signed_out, first, {})
            self.visits: list[str] = []

        def goto(self, url, **kwargs):
            self.visits.append(url)
            self.url = url

        def content(self):
            if "/auth/login" in (self.url or ""):
                return "<html><h1>Sign in</h1></html>"
            return signed_out

    page = _StaysOut()
    session = _ApplySession()
    session._pair = (_FakeContext([page]), page)
    with patch(
        "connectors.builtin.time.time", side_effect=_clock_past(_LOGIN_WAIT_S)
    ), patch("connectors.builtin.time.sleep"):
        with pytest.raises(ScrapeHalted):
            session.resolve({"id": "1", "listing_url": first})
    assert second not in page.visits


def test_headless_signed_out_waits_for_login(monkeypatch):
    monkeypatch.setenv("CLOAKBROWSER_HEADLESS", "1")
    from connectors.builtin import _LOGIN_WAIT_S

    html = 'jobPostInit({"job":{"applyUrl":"","isEasyApply":false},"isLoggedIn":false});'
    page = _FakePage(html, "https://builtin.com/job/x/1", {
        _LOGIN_APPLY_SELECTOR: _FakeLocator(1),
        "a[href*=\"/apply/\"]": _FakeLocator(0),
    })
    with patch(
        "connectors.builtin.time.time", side_effect=_clock_past(_LOGIN_WAIT_S)
    ):
        with pytest.raises(ScrapeHalted):
            _resolve_apply_on_page(
                _FakeContext([page]),
                page,
                "https://builtin.com/job/x/1",
                wait_for_login=True,
            )
    assert "/auth/login" in page.url


def test_hidden_sign_out_reopens_a_visible_window(monkeypatch):
    monkeypatch.setenv("CLOAKBROWSER_HEADLESS", "1")
    from connectors.builtin import _ApplySession

    signed_out = (
        'jobPostInit({"job":{"applyUrl":"","isEasyApply":false},"isLoggedIn":false});'
    )
    hidden = _FakePage(signed_out, "https://builtin.com/job/x/1", {})
    visible = _FakePage(signed_out, "about:blank", {})

    @contextmanager
    def _headed(headless=None):
        assert headless is False
        yield (_FakeContext([visible]), visible)

    session = _ApplySession()
    session._pair = (_FakeContext([hidden]), hidden)
    hidden_session = MagicMock()
    session._cm = hidden_session
    with patch("connectors.builtin._browser_session", _headed):
        shown = session._reveal_for_login()
    assert shown is visible
    hidden_session.__exit__.assert_called_once()


def test_resolve_same_tab_redirect_polls_offsite():
    html = 'jobPostInit({"job":{"applyUrl":"","isEasyApply":false}});'
    apply_btn = _FakeLocator(1)

    page = _FakePage(html, "https://builtin.com/job/x/1", {
        _LOGIN_APPLY_SELECTOR: _FakeLocator(0),
        _APPLY_CLICK_SELECTORS[0]: apply_btn,
    })

    def _go_offsite():
        page.url = "https://boards.greenhouse.io/acme/jobs/42"

    apply_btn._on_click = _go_offsite
    context = _FakeContext([page])
    url = _resolve_apply_on_page(context, page, "https://builtin.com/job/x/1")
    assert url == "https://boards.greenhouse.io/acme/jobs/42"
    assert apply_btn.clicked


@patch("connectors.builtin.remember_listing_urls")
@patch("connectors.builtin.unseen_listing_urls", side_effect=lambda urls, source: list(urls))
@patch("connectors.builtin.time.sleep")
@patch("connectors.builtin.exclusion_reason", return_value=None)
@patch(
    "connectors.builtin.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.builtin.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.builtin.max_job_age_days", return_value=2)
@patch("connectors.builtin.stored_seen_listings", return_value=[])
@patch("connectors.builtin.source_job_urls", return_value=[])
@patch("connectors.builtin.requests.get")
def test_fetch_stores_captured_apply_url(mock_get, *_patches):
    mock_get.return_value = _Resp(_page(
        [_card()],
        published=[("11268876", "2026-09-19T22:58:36")],
    ))
    apply_html = (
        'jobPostInit({"job":{"applyUrl":'
        '"https://boards.greenhouse.io/acme/jobs/9","isEasyApply":false}});'
    )

    with _cloak_fetch(lambda url: apply_html) as browser:
        jobs = BuiltinConnector().fetch_jobs()
    assert len(jobs) == 1
    assert jobs[0]["url"] == "https://boards.greenhouse.io/acme/jobs/9"
    assert jobs[0]["listing_url"].startswith("https://builtin.com/job/")
    urls = [c.args[0] for c in mock_get.call_args_list if c.args]
    assert urls == [listing_url(1, 3, path) for path in LISTING_PATHS]
    assert all("/apply/" not in u for u in urls)
    assert browser.visits
    assert all("/jobs/" not in u for u in browser.visits)


@patch("connectors.builtin.remember_listing_urls")
@patch("connectors.builtin.unseen_listing_urls", return_value=[])
@patch("connectors.builtin.time.sleep")
@patch("connectors.builtin.exclusion_reason", return_value=None)
@patch(
    "connectors.builtin.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.builtin.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.builtin.max_job_age_days", return_value=2)
@patch("connectors.builtin.stored_seen_listings", return_value=[])
@patch(
    "connectors.builtin.source_job_urls",
    return_value=[("11268876", "https://builtin.com/job/senior-machine-learning-engineer/11268876")],
)
@patch("connectors.builtin.update_job_url", return_value=True)
@patch("connectors.builtin.requests.get")
def test_fetch_opens_cloak_for_stored_listing_urls(mock_get, mock_update, *_patches):
    mock_get.return_value = _Resp(_page(
        [_card()],
        published=[("11268876", "2026-09-19T22:58:36")],
    ))
    apply_html = (
        'jobPostInit({"job":{"applyUrl":'
        '"https://jobs.ashbyhq.com/acme/z","isEasyApply":false}});'
    )
    stored = "https://builtin.com/job/senior-machine-learning-engineer/11268876"

    with _cloak_fetch(lambda url: apply_html) as browser:
        jobs = BuiltinConnector().fetch_jobs()
    assert stored in browser.visits
    assert all("/jobs/" not in u for u in browser.visits)
    assert jobs == []
    assert mock_update.call_args.args[:3] == (
        "builtin",
        "11268876",
        "https://jobs.ashbyhq.com/acme/z",
    )
