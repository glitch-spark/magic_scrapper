"""
Mocked tests for OmniJobsConnector.

Covers: search URL filters, card parse, location strings, engineering
title filter, newest-first stale stop, Pro-wall stop, employer apply URL,
checkpoint soft-empty, and normalize() shape. No live HTTP / Playwright.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from connectors.omnijobs import (
    OmniJobsConnector,
    _extract_cards,
    _is_checkpoint,
    _is_engineering_title,
    _location_text,
    _merge_detail,
    _next_listing_url,
    _page_is_newest_first,
    listing_url,
)


_NOW = datetime(2026, 9, 22, 18, 0, tzinfo=timezone.utc)
_CUTOFF = _NOW - timedelta(days=7)


def _card(
    job_id="2583691",
    title="Senior Full-Stack TypeScript Engineer",
    location="remote USA",
    opened="Opened 1h ago",
    company="Mirantis",
    employees="585",
) -> str:
    return f"""
<article class="job-card">
  <a href="/en/jobs/{job_id}">{title}</a>
  <span>Remote</span>
  <span>{opened}</span>
  <span>{location}</span>
  <span>{company} {employees} employees</span>
</article>
"""


def _listing_html(*cards: str, pro: bool = False, next_href: str = "") -> str:
    nxt = ""
    if next_href:
        nxt = f'<a rel="next" href="{next_href}">Next</a>'
    wall = ""
    if pro:
        wall = "<p>See every result, not just page one. Join OmniJobs Pro.</p>"
    return "<html><body>" + "".join(cards) + nxt + wall + "</body></html>"


def _detail_html(
    title="Senior Full-Stack TypeScript Engineer",
    company="Mirantis",
    description="<p>Build React services.</p>",
    date_posted="2026-09-22",
    apply="https://jobs.lever.co/mirantis/abc",
) -> str:
    import json

    payload = {
        "@context": "https://schema.org/",
        "@type": "JobPosting",
        "title": title,
        "datePosted": date_posted,
        "description": description,
        "hiringOrganization": {"@type": "Organization", "name": company},
        "jobLocationType": "TELECOMMUTE",
    }
    return (
        "<html><body>"
        f'<script type="application/ld+json">{json.dumps(payload)}</script>'
        f'<script>window.__DATA__ = {{"applicationUrl": "{apply}"}}</script>'
        f"<h1>{title}</h1>"
        "</body></html>"
    )


@contextmanager
def _fake_browser():
    yield MagicMock()


def test_listing_url_keeps_pasted_filters():
    url = listing_url(1)
    assert url.startswith("https://omnijobs.io/en/search?")
    assert "location=US" in url
    assert "location=ANYWHERE_IN_LATAM" in url
    assert "location=ANYWHERE_IN_WORLD" in url
    assert "location=ANYWHERE_IN_EUROPE" in url
    assert "locationType=remote" in url
    assert "jobFunction=software+development" in url or "jobFunction=software%20development" in url
    assert "page=" not in url
    assert "page=2" in listing_url(2)


def test_parse_cards_location_and_engineering_filter():
    html = _listing_html(
        _card(),
        _card(
            job_id="99",
            title="Account Executive",
            location="remote REMOTE",
            opened="Opened 2h ago",
            company="Acme",
        ),
        _card(
            job_id="77",
            title="Product Engineer",
            location="San Francisco California",
            opened="Opened 3h ago",
            company="Parallel Partners",
            employees="12",
        ),
    )
    jobs = _extract_cards(html, now=_NOW)
    assert [j["id"] for j in jobs] == ["2583691", "99", "77"]
    eng = jobs[0]
    assert eng["title"] == "Senior Full-Stack TypeScript Engineer"
    assert eng["company"] == "Mirantis"
    assert eng["location"] == "Remote (US)"
    assert isinstance(eng["location"], str)
    assert eng["listing_url"] == "https://omnijobs.io/en/jobs/2583691"
    assert eng["posted_date"] == _NOW - timedelta(hours=1)
    assert jobs[1]["location"] == "Remote"
    assert jobs[2]["location"] == "Remote, San Francisco, California"
    assert _is_engineering_title(eng["title"])
    assert not _is_engineering_title(jobs[1]["title"])
    assert _page_is_newest_first(jobs)


def test_location_helpers():
    assert _location_text("remote USA") == "Remote (US)"
    assert _location_text("remote REMOTE") == "Remote"
    assert _location_text("Remote Europe") == "Remote (Europe)"
    assert _location_text("Remote LATAM") == "Remote (LATAM)"


def test_checkpoint_html_yields_no_cards():
    html = "<html><title>Vercel Security Checkpoint</title><body>We're verifying your browser</body></html>"
    assert _is_checkpoint(html)
    assert _extract_cards(html, now=_NOW) == []


def test_pro_wall_blocks_next_link():
    html = _listing_html(
        _card(),
        pro=True,
        next_href="https://omnijobs.io/en/search?page=2",
    )
    assert _next_listing_url(html, listing_url(1)) is None


def test_next_link_without_pro_wall():
    html = _listing_html(
        _card(),
        next_href="/en/search?location=US&locationType=remote&jobFunction=software+development&page=2",
    )
    nxt = _next_listing_url(html, listing_url(1))
    assert nxt is not None
    assert "page=2" in nxt


def test_merge_detail_prefers_employer_apply():
    job = {
        "id": "2583691",
        "title": "Senior Full-Stack TypeScript Engineer",
        "company": "Mirantis",
        "location": "Remote (US)",
        "listing_url": "https://omnijobs.io/en/jobs/2583691",
        "url": "https://omnijobs.io/en/jobs/2583691",
        "description": "",
        "posted_date": _NOW - timedelta(hours=1),
    }
    assert _merge_detail(job, _detail_html(), _CUTOFF) is True
    assert job["url"] == "https://jobs.lever.co/mirantis/abc"
    assert "React" in job["description"]
    assert job["company"] == "Mirantis"


@patch("connectors.omnijobs.remember_listing_urls")
@patch(
    "connectors.omnijobs.unseen_listing_urls",
    side_effect=lambda urls, source, max_new=None: list(urls)[: (max_new or len(urls))],
)
@patch("connectors.omnijobs.time.sleep")
@patch("connectors.omnijobs.exclusion_reason", return_value=None)
@patch(
    "connectors.omnijobs.load_candidate_profile",
    return_value={"personal": {"location": "San Francisco, CA"}},
)
@patch("connectors.omnijobs.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.omnijobs.max_job_age_days", return_value=7)
@patch("connectors.omnijobs._browser_session", _fake_browser)
@patch("connectors.omnijobs._open_detail")
@patch("connectors.omnijobs._open_listing")
def test_fetch_stops_at_first_stale_on_newest_first(
    mock_listing, mock_detail, *_patches
):
    fresh = _card(job_id="1", opened="Opened 1h ago")
    stale = _card(
        job_id="2",
        title="Backend Engineer",
        opened="Opened 20d ago",
        company="Old Co",
    )
    mock_listing.return_value = _listing_html(fresh, stale)
    mock_detail.return_value = _detail_html()

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return _NOW

    with patch("connectors.omnijobs.datetime", _FrozenDateTime):
        jobs = OmniJobsConnector().fetch_jobs()
    assert [j["id"] for j in jobs] == ["1"]
    assert mock_listing.call_count == 1
    assert jobs[0]["url"].startswith("https://jobs.lever.co/")


@patch("connectors.omnijobs.remember_listing_urls")
@patch("connectors.omnijobs.unseen_listing_urls")
@patch("connectors.omnijobs.time.sleep")
@patch("connectors.omnijobs.job_age_cutoff", return_value=_CUTOFF)
@patch("connectors.omnijobs.max_job_age_days", return_value=7)
@patch("connectors.omnijobs._browser_session", _fake_browser)
@patch("connectors.omnijobs._open_detail")
@patch("connectors.omnijobs._open_listing", return_value="")
def test_fetch_soft_skips_checkpoint(mock_listing, mock_detail, *_patches):
    jobs = OmniJobsConnector().fetch_jobs()
    assert jobs == []
    mock_detail.assert_not_called()


class TestNormalize:
    def _raw(self):
        return {
            "id": "2583691",
            "listing_url": "https://omnijobs.io/en/jobs/2583691",
            "url": "https://jobs.lever.co/mirantis/abc",
            "title": "Senior Full-Stack TypeScript Engineer",
            "company": "Mirantis",
            "location": "Remote (US)",
            "description": "Build React services.",
            "posted_date": _NOW,
        }

    def test_shape(self):
        from tests.connectors.test_normalize import _assert_shape

        n = OmniJobsConnector().normalize(self._raw())
        _assert_shape(n, "omnijobs")

    def test_keeps_string_location(self):
        n = OmniJobsConnector().normalize(self._raw())
        assert n["location"] == "Remote (US)"
        assert isinstance(n["location"], str)
        assert n["external_id"] == "2583691"
        assert n["url"].startswith("https://jobs.lever.co/")
