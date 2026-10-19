"""Scraped job rows are mirrored to Mongo with the jobs-table columns."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import config
from models.database import Job
from utils.alerts import ScrapeHalted
from utils import mongo_jobs


def _reset():
    mongo_jobs._client = None
    mongo_jobs._jobs = None
    mongo_jobs._indexed = False
    mongo_jobs._unavailable = False
    mongo_jobs._missing_uri_logged = False


def _job() -> Job:
    return Job(
        id=7,
        external_id="builtin-7",
        source="builtin",
        company="Acme",
        title="Engineer",
        location="Remote",
        raw_location_text="Remote, US",
        description="<p>Build</p>",
        description_text="Build",
        url="https://jobs.example.com/7",
        company_key="acme",
        url_key="https://jobs.example.com/7",
        remote_eligibility="remote",
        ats_type="greenhouse",
        fit_score=None,
        posted_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
        status="new",
        archived=False,
    )


def test_unset_uri_does_not_open_mongo(monkeypatch):
    _reset()
    monkeypatch.setattr(config, "MONGODB_URI", "")
    collection = MagicMock()
    monkeypatch.setattr(mongo_jobs, "_jobs_collection", lambda: collection)
    mongo_jobs.mirror_job(_job())
    collection.replace_one.assert_not_called()


def test_mirror_replaces_the_same_columns(monkeypatch):
    _reset()
    monkeypatch.setattr(config, "MONGODB_URI", "mongodb://user:secret@db.example:27017")
    collection = MagicMock()
    monkeypatch.setattr(mongo_jobs, "_jobs_collection", lambda: collection)

    job = _job()
    mongo_jobs.mirror_job(job)

    collection.replace_one.assert_called_once()
    key, document = collection.replace_one.call_args.args[:2]
    assert key == {"external_id": "builtin-7"}
    assert collection.replace_one.call_args.kwargs["upsert"] is True
    assert set(document) == set(mongo_jobs._JOB_FIELDS)
    assert document["id"] == 7
    assert document["source"] == "builtin"
    assert document["url"] == "https://jobs.example.com/7"
    assert document["description_text"] == "Build"
    assert document["posted_date"] == job.posted_date
    assert "fit_score" not in document
    assert "archived" not in document
    assert "company_key" not in document


def test_mirror_failure_stops_the_scrape(monkeypatch):
    _reset()
    monkeypatch.setattr(config, "MONGODB_URI", "mongodb://localhost:27017")
    collection = MagicMock()
    collection.replace_one.side_effect = RuntimeError("down")
    monkeypatch.setattr(mongo_jobs, "_jobs_collection", lambda: collection)
    try:
        mongo_jobs.mirror_job(_job())
    except ScrapeHalted as exc:
        assert "MongoDB" in str(exc)
    else:
        raise AssertionError("mongo write failure did not stop the scrape")
