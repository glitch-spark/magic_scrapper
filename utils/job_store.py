"""Look up already-ingested listing URLs so sitemap crawlers skip known locs.

A count/page cap is only safe on a newest-first list. Unsorted sitemaps instead
walk (or batch) unseen URLs and drop stale jobs by posted date. ``jobs.url``
alone is not enough for that: stale/expired pages are never inserted, and
Remote100K stores the ATS apply URL rather than the sitemap loc. A small
``seen_listing_urls`` table records listing URLs we already crawled.
"""
from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

import config
from models.database import Job
from utils.alerts import ScrapeHalted, halt, is_db_write_error, is_skippable_db_lock
from utils.logger import setup_logger
from utils.mongo_jobs import mirror_job

logger = setup_logger("job_store")

_engine = None
_SessionLocal: sessionmaker | None = None


def configure_sqlite(engine) -> None:
    """Let the fetch and the scraper write the same file without blocking."""
    if getattr(getattr(engine, "dialect", None), "name", "") != "sqlite":
        return

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_connection, _record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()


def _session() -> Session | None:
    global _engine, _SessionLocal
    try:
        if _SessionLocal is None:
            connect_args = (
                {"timeout": 30, "check_same_thread": False}
                if str(config.DATABASE_URL).startswith("sqlite")
                else {}
            )
            _engine = create_engine(config.DATABASE_URL, connect_args=connect_args)
            configure_sqlite(_engine)
            _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
        return _SessionLocal()
    except Exception as e:
        logger.debug(f"job_store session unavailable: {e}")
        return None


def _ensure_seen_table(session: Session) -> None:
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS seen_listing_urls (
                source VARCHAR NOT NULL,
                url VARCHAR NOT NULL,
                PRIMARY KEY (source, url)
            )
            """
        )
    )
    session.commit()


def _norm_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def known_job_urls(source: str, *, include_seen_listings: bool = True) -> set[str]:
    """Return stored job URLs, and optionally listing URLs already crawled."""
    session = _session()
    if session is None:
        return set()
    try:
        _ensure_seen_table(session)
        rows = (
            session.query(Job.url)
            .filter(Job.source == source, Job.url.isnot(None))
            .all()
        )
        urls = {_norm_url(r[0]) for r in rows if r[0]}
        if include_seen_listings:
            seen = session.execute(
                text("SELECT url FROM seen_listing_urls WHERE source = :source"),
                {"source": source},
            )
            urls.update(_norm_url(r[0]) for r in seen if r[0])
        return {u for u in urls if u}
    except Exception as e:
        logger.debug(f"known_job_urls({source!r}) failed: {e}")
        return set()
    finally:
        session.close()


def known_external_ids(source: str) -> set[str]:
    """Return ``Job.external_id`` values for ``source`` (e.g. Remote100K slugs)."""
    session = _session()
    if session is None:
        return set()
    try:
        rows = (
            session.query(Job.external_id)
            .filter(Job.source == source, Job.external_id.isnot(None))
            .all()
        )
        return {str(r[0]).strip() for r in rows if r[0]}
    except Exception as e:
        logger.debug(f"known_external_ids({source!r}) failed: {e}")
        return set()
    finally:
        session.close()


def unseen_listing_urls(
    urls: Iterable[str],
    source: str,
    *,
    max_new: int | None = None,
    include_seen_listings: bool = True,
) -> list[str]:
    """Return sitemap/listing URLs not already stored or crawled.

    Matches ``Job.url`` (trailing slash ignored) or ``Job.external_id`` against
    the last path segment. When ``max_new`` is set, only that many unseen URLs
    are returned (leftovers stay in the sitemap for the next run).

    ``include_seen_listings=False`` ignores ``seen_listing_urls`` and only
    skips URLs already stored as jobs (live indexes that re-list the same
    locs after a crawl that stored nothing).
    """
    known_urls = known_job_urls(source, include_seen_listings=include_seen_listings)
    known_ids = known_external_ids(source)
    out: list[str] = []
    seen_this_run: set[str] = set()
    for url in urls:
        key = _norm_url(url)
        if not key or key in seen_this_run:
            continue
        slug = key.rsplit("/", 1)[-1]
        if key in known_urls or slug in known_ids:
            continue
        seen_this_run.add(key)
        out.append(url)
        if max_new is not None and len(out) >= max_new:
            break
    return out


def remember_listing_urls(source: str, urls: Iterable[str]) -> None:
    """Record listing URLs we already fetched so unsorted sitemaps do not refetch."""
    normalized = [_norm_url(u) for u in urls if _norm_url(u)]
    if not normalized:
        return
    session = _session()
    if session is None:
        return
    try:
        _ensure_seen_table(session)
        for url in normalized:
            session.execute(
                text(
                    "INSERT OR IGNORE INTO seen_listing_urls (source, url) "
                    "VALUES (:source, :url)"
                ),
                {"source": source, "url": url},
            )
        session.commit()
    except Exception as e:
        logger.debug(f"remember_listing_urls({source!r}) failed: {e}")
        try:
            session.rollback()
        except Exception:
            pass
    finally:
        session.close()


def jobs_with_url_host(source: str, host: str) -> list[tuple[str, str]]:
    """Return ``(external_id, url)`` for jobs whose URL is on ``host``."""
    session = _session()
    if session is None:
        return []
    needle = (host or "").lower().removeprefix("www.")
    if not needle:
        return []
    try:
        rows = (
            session.query(Job.external_id, Job.url)
            .filter(Job.source == source, Job.url.isnot(None))
            .all()
        )
        out: list[tuple[str, str]] = []
        for ext, url in rows:
            if not ext or not url:
                continue
            if needle in str(url).lower():
                out.append((str(ext), str(url)))
        return out
    except Exception as e:
        logger.debug(f"jobs_with_url_host({source!r}, {host!r}) failed: {e}")
        return []
    finally:
        session.close()


def source_job_urls(source: str) -> list[tuple[str, str]]:
    """Return ``(external_id, url)`` for every stored job from ``source``."""
    session = _session()
    if session is None:
        return []
    try:
        rows = (
            session.query(Job.external_id, Job.url)
            .filter(Job.source == source, Job.url.isnot(None))
            .all()
        )
        return [(str(ext), str(url)) for ext, url in rows if ext and url]
    except Exception as e:
        logger.debug(f"source_job_urls({source!r}) failed: {e}")
        return []
    finally:
        session.close()


def stored_seen_listings(source: str) -> list[str]:
    """Return listing URLs already recorded for ``source``."""
    session = _session()
    if session is None:
        return []
    try:
        _ensure_seen_table(session)
        rows = session.execute(
            text("SELECT url FROM seen_listing_urls WHERE source = :source"),
            {"source": source},
        )
        return [str(row[0]) for row in rows if row[0]]
    except Exception as e:
        logger.debug(f"stored_seen_listings({source!r}) failed: {e}")
        return []
    finally:
        session.close()


def update_job_url(
    source: str,
    external_id: str,
    url: str,
    *,
    ats_type: str | None = None,
) -> bool:
    """Set ``Job.url`` (and optional ``ats_type``) for one stored job."""
    session = _session()
    if session is None or not external_id or not url:
        return False
    try:
        job = (
            session.query(Job)
            .filter(Job.source == source, Job.external_id == str(external_id))
            .one_or_none()
        )
        if job is None:
            return False
        job.url = url
        if ats_type is not None:
            job.ats_type = ats_type
        session.commit()
        mirror_job(job)
        return True
    except ScrapeHalted:
        raise
    except Exception as e:
        logger.debug(f"update_job_url({source!r}, {external_id!r}) failed: {e}")
        if not is_skippable_db_lock(e) and is_db_write_error(e):
            halt("db_write", str(e), source="sqlite")
        try:
            session.rollback()
        except Exception:
            pass
        return False
    finally:
        session.close()
