"""Mocked tests for WeAreDistributedConnector — sitemap/detail retries."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from requests.exceptions import Timeout as RequestsTimeout

from connectors.wearedistributed import (
    WeAreDistributedConnector,
    _RETRIES,
    _extract_jsonld,
    _is_engineering_url,
    _parse_sitemap,
)


_FUTURE = (datetime.now(tz=timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")


def _sitemap_xml(*slugs: str) -> bytes:
    items = "\n".join(
        f"  <url><loc>https://wearedistributed.org/job/{s}</loc>"
        f"<lastmod>2026-09-21</lastmod></url>"
        for s in slugs
    )
    return (
        '<?xml version="1.0"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{items}</urlset>"
    ).encode()


def _job_html(title="Senior Engineer", company="Acme") -> str:
    ld = {
        "@context": "https://schema.org/",
        "@type": "JobPosting",
        "title": title,
        "datePosted": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
        "validThrough": _FUTURE,
        "hiringOrganization": {"@type": "Organization", "name": company},
        "applicantLocationRequirements": [{"@type": "Country", "name": "Worldwide"}],
        "description": "<p>Python role</p>",
    }
    return (
        f'<html><head><script type="application/ld+json">'
        f"{json.dumps(ld)}</script></head><body></body></html>"
    )


def _mock_response(content, status=200):
    m = MagicMock()
    m.status_code = status
    if isinstance(content, bytes):
        m.content = content
        m.text = content.decode()
    else:
        m.content = content.encode()
        m.text = content
    return m


def test_parse_sitemap_newest_first():
    xml = (
        b'<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        b"<url><loc>https://wearedistributed.org/job/old-engineer</loc>"
        b"<lastmod>2026-09-01</lastmod></url>"
        b"<url><loc>https://wearedistributed.org/job/new-engineer</loc>"
        b"<lastmod>2026-09-21</lastmod></url>"
        b"</urlset>"
    )
    assert _parse_sitemap(xml) == [
        "https://wearedistributed.org/job/new-engineer",
        "https://wearedistributed.org/job/old-engineer",
    ]


def test_engineering_url_filter():
    assert _is_engineering_url("https://wearedistributed.org/job/acme-senior-engineer")
    assert not _is_engineering_url("https://wearedistributed.org/job/acme-recruiter")


def test_extract_jsonld():
    raw = _extract_jsonld(
        _job_html("Staff Engineer", "DeepCo"),
        "https://wearedistributed.org/job/deepco-staff-engineer",
    )
    assert raw is not None
    assert raw["title"] == "Staff Engineer"
    assert raw["company"] == "DeepCo"


@patch("connectors.wearedistributed.remember_listing_urls")
@patch(
    "connectors.wearedistributed.unseen_listing_urls",
    side_effect=lambda urls, source, **kwargs: list(urls),
)
@patch("connectors.wearedistributed.time.sleep")
@patch("connectors.wearedistributed.requests.get")
def test_fetch_returns_jobs(mock_get, _sleep, *_patches):
    mock_get.side_effect = [
        _mock_response(_sitemap_xml("acme-senior-engineer")),
        _mock_response(_job_html("Senior Engineer", "Acme")),
    ]
    jobs = WeAreDistributedConnector().fetch_jobs()
    assert len(jobs) == 1
    assert jobs[0]["title"] == "Senior Engineer"


@patch("connectors.wearedistributed.remember_listing_urls")
@patch("connectors.wearedistributed.unseen_listing_urls")
@patch("connectors.wearedistributed.time.sleep")
@patch("connectors.wearedistributed.requests.get")
def test_sitemap_timeout_retries_then_empty(mock_get, _sleep, mock_unseen, *_patches):
    mock_get.side_effect = RequestsTimeout("read timeout=40")
    assert WeAreDistributedConnector().fetch_jobs() == []
    assert mock_get.call_count == _RETRIES
    mock_unseen.assert_not_called()


@patch("connectors.wearedistributed.remember_listing_urls")
@patch(
    "connectors.wearedistributed.unseen_listing_urls",
    side_effect=lambda urls, source, **kwargs: list(urls),
)
@patch("connectors.wearedistributed.time.sleep")
@patch("connectors.wearedistributed.requests.get")
def test_detail_timeout_skips_and_continues(mock_get, _sleep, *_patches):
    mock_get.side_effect = [
        _mock_response(_sitemap_xml("slow-engineer", "ok-engineer")),
        *([RequestsTimeout("timeout")] * _RETRIES),
        _mock_response(_job_html("OK Engineer", "OK Co")),
    ]
    jobs = WeAreDistributedConnector().fetch_jobs()
    assert len(jobs) == 1
    assert jobs[0]["company"] == "OK Co"
    assert mock_get.call_count == 1 + _RETRIES + 1
