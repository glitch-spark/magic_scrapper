"""Mirror a scraped jobs-table row into MongoDB.

Writes happen only after SQLite has stored the same row. Scoring, review,
and other later edits stay in SQLite. An unset ``MONGODB_URI`` leaves
SQLite as the only store. A Mongo failure does not undo the SQLite write.
"""
from __future__ import annotations

from datetime import date, datetime
from urllib.parse import urlsplit

import config
from utils.alerts import halt
from utils.logger import setup_logger

logger = setup_logger("mongo_jobs")

_client = None
_jobs = None
_indexed = False
_unavailable = False
_missing_uri_logged = False


def mirror_job(job) -> None:
    """Upsert one jobs row. No-op when MongoDB is not configured."""
    if not (config.MONGODB_URI or "").strip():
        _log_missing_uri()
        return
    document = job_document(job)
    key = _upsert_key(document)
    if key is None:
        logger.debug("mongo mirror skipped a job with no external_id or url")
        return
    collection = _jobs_collection()
    if collection is None:
        return
    try:
        collection.replace_one(key, document, upsert=True)
    except Exception as e:
        logger.warning(f"mongo mirror failed for {key}: {e}")
        halt(
            "db_write",
            f"MongoDB did not store the scraped job: {e}",
            source="mongodb",
        )


_JOB_FIELDS = (
    "id",
    "external_id",
    "source",
    "company",
    "title",
    "location",
    "raw_location_text",
    "description",
    "url",
    "remote_eligibility",
    "ats_type",
    "posted_date",
    "created_at",
    "updated_at",
    "status",
    "description_text",
)


def job_document(job) -> dict:
    """Scrape fields only. Scores, resume, and archive columns stay in SQLite."""
    return {
        name: _bson_value(getattr(job, name))
        for name in _JOB_FIELDS
    }


def _bson_value(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return value


def _upsert_key(document: dict) -> dict | None:
    external_id = document.get("external_id")
    if external_id:
        return {"external_id": external_id}
    url = document.get("url")
    if url:
        return {"url": url}
    return None


def _jobs_collection():
    global _client, _jobs, _indexed, _unavailable
    if _unavailable:
        return None
    if _jobs is not None:
        return _jobs
    uri = (config.MONGODB_URI or "").strip()
    if not uri:
        return None
    try:
        from pymongo import MongoClient

        _client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        _jobs = _client[config.MONGODB_DB][config.MONGODB_JOBS_COLLECTION]
        if not _indexed:
            _jobs.create_index(
                "external_id",
                unique=True,
                partialFilterExpression={"external_id": {"$type": "string"}},
            )
            _indexed = True
        logger.info(
            "mirroring scraped jobs to "
            f"{_target_for_log(uri)} "
            f"{config.MONGODB_DB}.{config.MONGODB_JOBS_COLLECTION}"
        )
        return _jobs
    except Exception as e:
        logger.warning(f"mongo mirror unavailable: {e}")
        _unavailable = True
        halt(
            "db_write",
            f"MongoDB is unavailable, so the scrape cannot keep its result: {e}",
            source="mongodb",
        )
        _client = None
        _jobs = None
        return None


def _log_missing_uri() -> None:
    global _missing_uri_logged
    if _missing_uri_logged:
        return
    _missing_uri_logged = True
    logger.debug("MONGODB_URI is unset; scraped jobs stay in SQLite only")


def _target_for_log(uri: str) -> str:
    parts = urlsplit(uri)
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{host}{port}"
