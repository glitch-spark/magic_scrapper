"""
Mocked tests for RemoteYeahConnector.

Covers: listing-path RSS, item parse, location as a string, engineering
title filter, newest-first first-stale stop, utm strip, skip ineligible
before persist, no detail HTTP, and normalize() shape. No live HTTP.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from requests.exceptions import ConnectionError as RequestsConnectionError

from connectors.remoteyeah import (
    FEED_URL,
    RemoteYeahConnector,
    _csrf_token,
    _follow_apply_post_headers,
    _is_engineering_title,
    _offsite_apply_url,
    _parse_item,
)


_NOW = datetime(2026, 9, 21, 18, 0, tzinfo=timezone.utc)
_CUTOFF = _NOW - timedelta(days=2)


def _item(
    *,
    job_id="remote-senior-software-engineer-acme",
    title="Remote Senior Software Engineer at Acme",
    company="Acme",
    pub="2026-09-21T17:31:15+00:00",
    location="United States (Remote)",
    employment="Full-time",
    with_utm=True,
):
    link = f"https://remoteyeah.com/jobs/{job_id}"
    if with_utm:
        link += "?utm_source=rss&ref=rss"
    return f"""
<item>
  <title> {title} </title>
  <company> {company} </company>
  <link>{link}</link>
  <pubDate>{pub}</pubDate>
  <description><![CDATA[
    <ul>
      <li>Skills: Python, SQL</li>
      <li>Experiences: Senior</li>
      <li>Employments: {employment}</li>
      <li>Locations: {location}</li>
    </ul>
    <h2>Description:</h2>
    <p>Build systems.</p>
  ]]></description>
</item>
"""


def _feed(*items: str) -> str:
    return (
        '<?xml version="1.0"?><rss version="2.0">'
        "<channel><title>RemoteYeah</title>"
        # Intentionally broken amp (mirrors live feed) — regex parse must survive.
        '<atom:link href="https://remoteyeah.com/x.xml?utm_source=rss&ref=rss" />'
        + "".join(items)
        + "</channel></rss>"
    )


class _Resp:
    def __init__(self, text="", status=200, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.text = text


def test_parse_item_strips_utm_and_keeps_string_location():
    job = _parse_item(_item())
    assert job is not None
    assert job["title"] == "Senior Software Engineer"
    assert job["company"] == "Acme"
    assert job["location"] == "United States (Remote)"
    assert isinstance(job["location"], str)
    assert job["listing_url"] == (
        "https://remoteyeah.com/jobs/remote-senior-software-engineer-acme"
    )
    assert "utm_" not in job["url"]
    assert job["posted_date"] == datetime(2026, 9, 21, 17, 31, 15, tzinfo=timezone.utc)


def test_engineering_title_filter():
    assert _is_engineering_title("Senior Software Engineer")
    assert _is_engineering_title("Platform Engineer")
    assert _is_engineering_title("iOS Developer")
    assert _is_engineering_title("Prompt Engineer")
    assert _is_engineering_title("Data Analyst")
    assert not _is_engineering_title(
        "Marketing Analytics & Data Visualization Architect"
    )


@patch("connectors.remoteyeah.remember_listing_urls")
@patch(
    "connectors.remoteyeah.unseen_listing_urls",
    side_effect=lambda urls, source: list(urls),
)
@patch("connectors.remoteyeah.time.sleep")
@patch("connectors.remoteyeah.exclusion_reason", return_value=None)
@patch(
    "connectors.remoteyeah.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.remoteyeah.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.remoteyeah.max_job_age_days", return_value=2)
@patch("connectors.remoteyeah._chrome_user_data_dir", return_value="")
@patch("connectors.remoteyeah.requests.get")
def test_fetch_stops_at_first_stale_and_skips_non_eng(mock_get, *_mocks):
    body = _feed(
        _item(
            job_id="fresh-eng",
            title="Remote Backend Engineer at Acme",
            pub="2026-09-21T12:00:00+00:00",
        ),
        _item(
            job_id="fresh-mkt",
            title="Remote Marketing Analytics Architect at BrandCo",
            company="BrandCo",
            pub="2026-09-21T11:00:00+00:00",
        ),
        _item(
            job_id="stale-eng",
            title="Remote Staff Software Engineer at OldCo",
            company="OldCo",
            pub="2026-09-10T12:00:00+00:00",
        ),
        _item(
            job_id="after-stale",
            title="Remote AI Engineer at LaterCo",
            company="LaterCo",
            pub="2026-09-09T12:00:00+00:00",
        ),
    )
    mock_get.return_value = _Resp(body)
    jobs = RemoteYeahConnector().fetch_jobs()
    assert len(jobs) == 1
    assert jobs[0]["title"] == "Backend Engineer"
    assert jobs[0]["url"].startswith("https://remoteyeah.com/jobs/")
    assert mock_get.call_args.args[0] == FEED_URL


@patch("connectors.remoteyeah.remember_listing_urls")
@patch(
    "connectors.remoteyeah.unseen_listing_urls",
    side_effect=lambda urls, source: list(urls),
)
@patch("connectors.remoteyeah.time.sleep")
@patch("connectors.remoteyeah.exclusion_reason", return_value=None)
@patch(
    "connectors.remoteyeah.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.remoteyeah.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.remoteyeah.max_job_age_days", return_value=2)
@patch("connectors.remoteyeah._chrome_user_data_dir", return_value="")
@patch("connectors.remoteyeah.requests.get")
def test_retries_then_succeeds(mock_get, *_mocks):
    body = _feed(
        _item(title="Remote Software Engineer at Acme", pub="2026-09-21T12:00:00+00:00")
    )
    mock_get.side_effect = [
        RequestsConnectionError("boom"),
        RequestsConnectionError("boom"),
        _Resp(body),
    ]
    jobs = RemoteYeahConnector().fetch_jobs()
    assert len(jobs) == 1


@patch("connectors.remoteyeah.remember_listing_urls")
@patch(
    "connectors.remoteyeah.unseen_listing_urls",
    side_effect=lambda urls, source: list(urls),
)
@patch("connectors.remoteyeah.time.sleep")
@patch(
    "connectors.remoteyeah.exclusion_reason",
    return_value="location",
)
@patch(
    "connectors.remoteyeah.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.remoteyeah.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.remoteyeah.max_job_age_days", return_value=2)
@patch("connectors.remoteyeah._chrome_user_data_dir", return_value="")
@patch("connectors.remoteyeah.requests.get")
def test_skips_ineligible_before_persist(mock_get, *_mocks):
    body = _feed(
        _item(
            title="Remote Software Engineer at Acme",
            location="Europe Only (Remote)",
            pub="2026-09-21T12:00:00+00:00",
        )
    )
    mock_get.return_value = _Resp(body)
    assert RemoteYeahConnector().fetch_jobs() == []


def test_normalize_shape():
    raw = {
        "id": "remote-senior-software-engineer-acme",
        "listing_url": "https://remoteyeah.com/jobs/remote-senior-software-engineer-acme",
        "url": "https://remoteyeah.com/jobs/remote-senior-software-engineer-acme",
        "title": "Senior Software Engineer",
        "company": "Acme",
        "location": "United States (Remote)",
        "description": "<p>Build systems.</p>",
        "posted_date": datetime(2026, 9, 21, 17, 31, 15, tzinfo=timezone.utc),
    }
    n = RemoteYeahConnector().normalize(raw)
    assert n["source"] == "remoteyeah"
    assert n["external_id"] == "remote-senior-software-engineer-acme"
    assert n["company"] == "Acme"
    assert n["title"] == "Senior Software Engineer"
    assert n["location"] == "United States (Remote)"
    assert isinstance(n["location"], str)
    assert n["raw_location_text"] == "United States (Remote)"
    assert n["url"].startswith("https://remoteyeah.com/jobs/")
    assert "ats_type" in n
    assert n["posted_date"] == raw["posted_date"]
    assert n["remote_eligibility"] is None


def test_offsite_apply_url_rejects_remoteyeah():
    assert _offsite_apply_url("https://remoteyeah.com/jobs/x/apply") == ""
    assert _offsite_apply_url(
        "https://remoteyeah.com/login?redirect_url=https://remoteyeah.com/jobs/x"
    ) == ""
    assert _offsite_apply_url(
        "https://boards.greenhouse.io/acme/jobs/1?utm_source=remoteyeah"
    ) == "https://boards.greenhouse.io/acme/jobs/1"


def test_csrf_token_from_laravel_form():
    html = (
        '<form method="post" action="https://remoteyeah.com/jobs/x/apply">'
        '<input type="hidden" name="_token" value="abcTOKEN" autocomplete="off">'
        "<button>Apply now</button></form>"
    )
    assert _csrf_token(html) == "abcTOKEN"


class _FakeResponse:
    def __init__(self, status, location=None, url=""):
        self.status = status
        self.url = url
        self.headers = {"location": location} if location else {}


class _FakeAPIRequest:
    def __init__(self, hops=None):
        self.hops = hops or {}
        self.fetched = []
        self.posted = []

    def post(self, url, **kwargs):
        self.posted.append((url, kwargs.get("form")))
        status, location = self.hops.get(("POST", url), (200, None))
        return _FakeResponse(status, location, url)

    def get(self, url, **kwargs):
        self.fetched.append(url)
        status, location = self.hops.get(("GET", url), (200, None))
        return _FakeResponse(status, location, url)


class _FakeContext:
    def __init__(self, hops=None):
        self.request = _FakeAPIRequest(hops)


def test_follow_apply_post_headers_returns_offsite_location():
    listing = "https://remoteyeah.com/jobs/x"
    apply_url = listing + "/apply"
    ats = "https://jobs.ashbyhq.com/acme/abc"
    context = _FakeContext({("POST", apply_url): (302, ats)})
    assert _follow_apply_post_headers(context, listing, apply_url, "tok") == ats
    assert ats not in context.request.fetched
    assert context.request.posted == [(apply_url, {"_token": "tok"})]


def test_follow_apply_post_headers_login_keeps_listing():
    listing = "https://remoteyeah.com/jobs/x"
    apply_url = listing + "/apply"
    context = _FakeContext({
        ("POST", apply_url): (
            302,
            "https://remoteyeah.com/login?redirect_url=" + listing,
        ),
    })
    assert _follow_apply_post_headers(context, listing, apply_url, "tok") == ""


@patch("connectors.remoteyeah.remember_listing_urls")
@patch(
    "connectors.remoteyeah.unseen_listing_urls",
    side_effect=lambda urls, source: list(urls),
)
@patch("connectors.remoteyeah.time.sleep")
@patch("connectors.remoteyeah.exclusion_reason", return_value=None)
@patch(
    "connectors.remoteyeah.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.remoteyeah.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.remoteyeah.max_job_age_days", return_value=2)
@patch("connectors.remoteyeah.jobs_with_url_host", return_value=[])
@patch("connectors.remoteyeah._chrome_user_data_dir", return_value="C:/chrome-remoteyeah")
@patch("connectors.remoteyeah.requests.get")
def test_fetch_stores_captured_apply_url(mock_get, *_mocks):
    mock_get.return_value = _Resp(_feed(
        _item(title="Remote Software Engineer at Acme", pub="2026-09-21T12:00:00+00:00")
    ))

    class _FakePage:
        def goto(self, url, **kwargs):
            self.url = url

        def content(self):
            return (
                '<form method="post" action="https://remoteyeah.com/jobs/x/apply">'
                '<input type="hidden" name="_token" value="tok123">'
                "</form>"
            )

    @contextmanager
    def _session():
        listing = "https://remoteyeah.com/jobs/remote-senior-software-engineer-acme"
        apply_url = listing + "/apply"
        yield (
            _FakeContext({
                ("POST", apply_url): (302, "https://jobs.lever.co/acme/1"),
            }),
            _FakePage(),
        )

    with patch("connectors.remoteyeah._browser_session", _session):
        jobs = RemoteYeahConnector().fetch_jobs()
    assert len(jobs) == 1
    assert jobs[0]["url"] == "https://jobs.lever.co/acme/1"
    assert jobs[0]["listing_url"].startswith("https://remoteyeah.com/jobs/")
    assert mock_get.call_args.args[0] == FEED_URL
