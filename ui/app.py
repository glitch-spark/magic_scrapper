"""
Job Apply Agent — local web UI (FastAPI).

Start via:  python run_pipeline.py ui
Or directly: uvicorn ui.app:app --port 7860
"""
import asyncio
import json
import re
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
import utils.ssl_compat  # noqa: F401  — trust OS CAs for requests HTTPS

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError:  # pragma: no cover — apscheduler optional at import time
    BackgroundScheduler = None  # type: ignore[assignment,misc]
    CronTrigger = None  # type: ignore[assignment]
    IntervalTrigger = None  # type: ignore[assignment]
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import case, create_engine, func, or_, update
from sqlalchemy.orm import defer, sessionmaker

import config
from models.database import CompanyProfile, InterviewPrepSheet, Job, ensure_company_profiles, ensure_job_columns
from utils.dedup import backfill_dedup_keys
from utils.job_store import configure_sqlite
from utils.application_filter import (
    applied_to_same_company_within_days,
    recent_applied_company_keys,
)
from utils.compensation import extract_compensation
from utils.company_research import company_name_key, is_real_company_name
from utils.scoring import REJECT_LABELS, parse_score_breakdown
from utils.text_cleaning import sanitize_skill_object_dumps, clean_description

# ---------------------------------------------------------------------------
# App + DB
# ---------------------------------------------------------------------------

app = FastAPI(title="Job Apply Agent UI")

_sqlite = str(config.DATABASE_URL).startswith("sqlite")
_engine = create_engine(
    config.DATABASE_URL,
    connect_args={"timeout": 30, "check_same_thread": False} if _sqlite else {},
)
configure_sqlite(_engine)
_Session = sessionmaker(bind=_engine)
if _engine.dialect.name == "sqlite":
    ensure_job_columns(_engine)
    backfill_dedup_keys(_engine)
    ensure_company_profiles(_engine)

HTML_PATH = Path(__file__).parent / "index.html"


def _db():
    s = _Session()
    try:
        yield s
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Pipeline state (in-memory, single-user local tool)
# ---------------------------------------------------------------------------

_pipeline: Dict[str, Any] = {
    "status": "idle",   # idle | running | done | failed
    "started_at": None,
    "finished_at": None,
    "steps": {
        "fetch":    {"status": "pending", "detail": ""},
        "evaluate": {"status": "pending", "detail": ""},
        "analyze":  {"status": "pending", "detail": ""},
    },
    "log": [],
    "error": None,
}
_pipeline_lock = threading.Lock()


def _run_pipeline_subprocess():
    global _pipeline
    python = sys.executable
    cmd = [python, "run_pipeline.py", "full-run", "--email"]

    with _pipeline_lock:
        _pipeline["status"] = "running"
        _pipeline["started_at"] = datetime.utcnow().isoformat()
        _pipeline["finished_at"] = None
        _pipeline["error"] = None
        _pipeline["log"] = []
        _pipeline["steps"] = {
            "fetch":    {"status": "pending", "detail": ""},
            "evaluate": {"status": "pending", "detail": ""},
            "analyze":  {"status": "pending", "detail": ""},
        }

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        for line in proc.stdout:
            line = line.rstrip()
            with _pipeline_lock:
                _pipeline["log"].append(line)
                _update_steps_from_log(line)

        proc.wait()
        with _pipeline_lock:
            if proc.returncode == 0:
                _pipeline["status"] = "done"
                for step in _pipeline["steps"].values():
                    if step["status"] != "done":
                        step["status"] = "done"
            else:
                _pipeline["status"] = "failed"
                _pipeline["error"] = f"Exit code {proc.returncode}"
    except Exception as exc:
        with _pipeline_lock:
            _pipeline["status"] = "failed"
            _pipeline["error"] = str(exc)
    finally:
        with _pipeline_lock:
            _pipeline["finished_at"] = datetime.utcnow().isoformat()


def _update_steps_from_log(line: str):
    """Heuristically map log lines to step states (no lock needed — caller holds it)."""
    low = line.lower()
    steps = _pipeline["steps"]

    if "fetching jobs" in low or "fetch" in low and "source" in low:
        steps["fetch"]["status"] = "running"
    elif "successfully fetched" in low:
        steps["fetch"]["status"] = "done"
        steps["fetch"]["detail"] = line.split("INFO")[-1].strip() if "INFO" in line else line
        steps["evaluate"]["status"] = "running"
    elif "evaluating" in low or "scoring" in low or "evaluate" in low:
        steps["evaluate"]["status"] = "running"
    elif "successfully evaluated" in low or "evaluation complete" in low:
        steps["evaluate"]["status"] = "done"
        steps["analyze"]["status"] = "running"
    elif "analyzing" in low or "llm" in low and "job" in low:
        steps["analyze"]["status"] = "running"
    elif "full pipeline run complete" in low:
        steps["fetch"]["status"] = "done"
        steps["evaluate"]["status"] = "done"
        steps["analyze"]["status"] = "done"


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------

_SCHEDULE_PATH = Path(__file__).parent.parent / "ui_schedule.json"
_DEFAULT_SCHEDULE: Dict[str, Any] = {"mode": "off", "interval_hours": 4, "times": []}
_sched_config: Dict[str, Any] = dict(_DEFAULT_SCHEDULE)
_scheduler = BackgroundScheduler(timezone="UTC") if BackgroundScheduler else None


def _load_sched_config() -> None:
    global _sched_config
    if _SCHEDULE_PATH.exists():
        try:
            _sched_config = json.loads(_SCHEDULE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass


def _save_sched_config() -> None:
    _SCHEDULE_PATH.write_text(json.dumps(_sched_config, indent=2), encoding="utf-8")


def _scheduled_run() -> None:
    with _pipeline_lock:
        if _pipeline["status"] == "running":
            return
    threading.Thread(target=_run_pipeline_subprocess, daemon=True).start()


def _apply_schedule() -> None:
    if not _scheduler:
        return
    _scheduler.remove_all_jobs()
    mode = _sched_config.get("mode", "off")
    if mode == "interval":
        hours = max(1, int(_sched_config.get("interval_hours", 4)))
        _scheduler.add_job(_scheduled_run, IntervalTrigger(hours=hours), id="pi")
    elif mode == "daily":
        for i, t in enumerate(_sched_config.get("times", [])):
            try:
                h, m = t.strip().split(":")
                _scheduler.add_job(_scheduled_run, CronTrigger(hour=int(h), minute=int(m)), id=f"pd_{i}")
            except Exception:
                pass


def _next_run_iso() -> str:
    if not _scheduler:
        return ""
    runs = [j.next_run_time for j in _scheduler.get_jobs() if j.next_run_time]
    return min(runs).isoformat() if runs else ""


def _read_task_scheduler() -> Dict[str, Any]:
    try:
        result = subprocess.run(
            ["schtasks", "/query", "/fo", "CSV", "/v"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.splitlines():
            if "run_pipeline" in line.lower():
                name = line.split('","')[0].strip('"').strip(",\"")
                return {"found": True, "name": name}
    except Exception:
        pass
    return {"found": False, "name": ""}


@app.on_event("startup")
def _on_startup() -> None:
    _load_sched_config()
    _apply_schedule()
    if _scheduler:
        _scheduler.start()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AVATAR_COLORS = [
    "linear-gradient(135deg,#4f8ef7,#7fb3ff)",
    "linear-gradient(135deg,#10b981,#34d399)",
    "linear-gradient(135deg,#f59e0b,#fbbf24)",
    "linear-gradient(135deg,#8b5cf6,#a78bfa)",
    "linear-gradient(135deg,#ef4444,#f87171)",
    "linear-gradient(135deg,#ec4899,#f472b6)",
    "linear-gradient(135deg,#06b6d4,#67e8f9)",
    "linear-gradient(135deg,#f97316,#fb923c)",
    "linear-gradient(135deg,#6366f1,#818cf8)",
    "linear-gradient(135deg,#14b8a6,#2dd4bf)",
]


def _avatar_color(company: str) -> str:
    return _AVATAR_COLORS[hash(company or "") % len(_AVATAR_COLORS)]


def _avatar_text(company: str) -> str:
    words = (company or "?").split()
    if len(words) >= 2:
        return (words[0][0] + words[1][0]).upper()
    return (company or "?")[:2].upper()


def _parse_json_list(raw) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except (json.JSONDecodeError, TypeError):
        return [s.strip() for s in str(raw).split(",") if s.strip()]


def _plain_str(value) -> str:
    return value if isinstance(value, str) else ""


EVAL_LABELS: dict[str, str] = {
    "location": "Location",
    "llm_shortlist": "LLM shortlist",
    "llm_review": "LLM review",
    "llm_reject": "LLM reject",
    "unanalyzed": "Not analyzed",
}


def _eval_bucket(job: Job) -> tuple[str, str]:
    """Primary evaluation chip for review / shortlisted list view."""
    remote = (job.remote_eligibility or "").strip().lower()
    if remote == "review":
        return "location", EVAL_LABELS["location"]
    rec = (job.recommendation or "").strip().lower()
    if rec in ("shortlist", "shortlisted"):
        return "llm_shortlist", EVAL_LABELS["llm_shortlist"]
    if rec == "review":
        return "llm_review", EVAL_LABELS["llm_review"]
    if rec in ("reject", "rejected"):
        return "llm_reject", EVAL_LABELS["llm_reject"]
    return "unanalyzed", EVAL_LABELS["unanalyzed"]


def _job_to_dict(
    job: Job,
    company_website: str = "",
    *,
    include_body: bool = True,
    applied_company_keys: Optional[set] = None,
) -> Dict[str, Any]:
    score = job.llm_fit_score if job.llm_fit_score is not None else job.fit_score
    reject_code = _plain_str(job.reject_code) or None
    display = ""
    salary = None
    equity = None
    if include_body:
        plain = sanitize_skill_object_dumps(job.description_text or job.description or "")
        display = sanitize_skill_object_dumps(job.description or job.description_text or "")
        compensation = extract_compensation(plain)
        salary = compensation.salary
        equity = compensation.equity
    eval_code, eval_label = _eval_bucket(job)
    company_key = company_name_key(job.company or "")
    applied_same_company = bool(
        is_real_company_name(job.company or "")
        and applied_company_keys
        and company_key
        and company_key in applied_company_keys
    )
    return {
        "id": job.id,
        "title": job.title or "",
        "company": job.company or "",
        "company_website": company_website or "",
        "applied_same_company": applied_same_company,
        "applied_to_same_company_within": applied_to_same_company_within_days(),
        "location": job.raw_location_text or job.location or "Remote",
        "source": job.source or "",
        "status": job.status or "new",
        "archived": bool(job.archived),
        "fit_score": score,
        "rule_score": job.fit_score,
        "llm_confidence": job.llm_confidence,
        "recommendation": job.recommendation,
        "strengths": _parse_json_list(job.llm_strengths) if include_body else [],
        "gaps": _parse_json_list(job.skill_gaps) if include_body else [],
        "reasoning": (job.fit_explanation or "") if include_body else "",
        "cover_letter": (job.cover_letter or "") if include_body else "",
        "description": display,
        "salary": salary,
        "equity": equity,
        "url": job.url or "",
        "posted_date": job.posted_date.isoformat() if job.posted_date else None,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
        "avatar_color": _avatar_color(job.company),
        "avatar_text": _avatar_text(job.company),
        "reject_code": reject_code,
        "reject_label": REJECT_LABELS.get(reject_code or "", "Unknown") if (job.status == "rejected" or reject_code) else None,
        "reject_detail": _plain_str(job.reject_detail) or None,
        "rule_status": _plain_str(job.rule_status) or None,
        "llm_status": _plain_str(job.llm_status) or None,
        "eval_code": eval_code,
        "eval_label": eval_label,
        "remote_eligibility": _plain_str(job.remote_eligibility) or None,
        "score_breakdown": parse_score_breakdown(getattr(job, "score_breakdown", None)),
    }


def _company_website_map(session, companies: List[str]) -> Dict[str, str]:
    keys = {company_name_key(c) for c in companies if company_name_key(c)}
    if not keys:
        return {}
    rows = (
        session.query(CompanyProfile.name_key, CompanyProfile.website_url)
        .filter(
            CompanyProfile.name_key.in_(keys),
            CompanyProfile.status == "completed",
        )
        .all()
    )
    return {key: url for key, url in rows if url}


def _job_to_dict_with_site(session, job: Job) -> Dict[str, Any]:
    sites = _company_website_map(session, [job.company or ""])
    return _job_to_dict(
        job,
        sites.get(company_name_key(job.company or ""), ""),
        applied_company_keys=recent_applied_company_keys(session),
    )


# ---------------------------------------------------------------------------
# Routes — static
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    if not HTML_PATH.exists():
        raise HTTPException(500, "index.html not found")
    return HTML_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Routes — data
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def stats():
    session = _Session()
    try:
        visible = _not_archived()
        rows = (
            session.query(Job.status, func.count())
            .filter(visible)
            .group_by(Job.status)
            .all()
        )
        counts = {status: n for status, n in rows if status != "archived"}
        archived_n = (
            session.query(func.count(Job.id)).filter(Job.archived.is_(True)).scalar() or 0
        )
        counts["archived"] = archived_n
        return {
            "counts": counts,
            "total": sum(counts.values()),
            "reject_stale_days": config.REJECT_STALE_DAYS,
        }
    finally:
        session.close()


_DASHBOARD_STATUSES = (
    "new",
    "review",
    "shortlisted",
    "applied",
    "deferred",
    "rejected",
    "expired",
)


def _not_archived():
    return or_(Job.archived.is_(False), Job.archived.is_(None))


def _dashboard_day_key(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value)
    return text[:10] if len(text) >= 10 else None


def dashboard_payload(session, *, now: datetime | None = None, days: int = 7) -> Dict[str, Any]:
    """Jobs published in the last week, stacked by status, plus active connectors."""
    now = now or datetime.utcnow()
    if now.tzinfo is not None:
        now = now.replace(tzinfo=None)
    today = now.date()
    day_list = [today - timedelta(days=offset) for offset in range(days - 1, -1, -1)]
    since = datetime.combine(day_list[0], datetime.min.time())

    week_rows = (
        session.query(Job.posted_date, Job.status)
        .filter(Job.posted_date.isnot(None), Job.posted_date >= since)
        .all()
    )
    by_day: Dict[str, Dict[str, int]] = {
        d.isoformat(): {status: 0 for status in _DASHBOARD_STATUSES} for d in day_list
    }
    for posted_date, status in week_rows:
        key = _dashboard_day_key(posted_date)
        if key not in by_day:
            continue
        status_key = status or "new"
        by_day[key][status_key] = by_day[key].get(status_key, 0) + 1

    stacked = []
    for d in day_list:
        key = d.isoformat()
        counts = {status: by_day[key].get(status, 0) for status in _DASHBOARD_STATUSES}
        counts.update({
            status: n
            for status, n in by_day[key].items()
            if status not in _DASHBOARD_STATUSES and n
        })
        stacked.append({
            "date": key,
            "label": f"{d.strftime('%a')} {d.month}/{d.day}",
            "counts": counts,
            "total": sum(counts.values()),
        })

    source_rows = (
        session.query(Job.source, func.count(Job.id))
        .filter(Job.status != "rejected", _not_archived())
        .group_by(Job.source)
        .order_by(func.count(Job.id).desc(), Job.source.asc())
        .limit(20)
        .all()
    )
    connectors = [
        {"source": (source or "unknown").strip() or "unknown", "count": n}
        for source, n in source_rows
        if n
    ]
    return {
        "days": days,
        "since": since.isoformat(),
        "until": now.isoformat(),
        "statuses": list(_DASHBOARD_STATUSES),
        "by_day": stacked,
        "connectors": connectors,
        "week_total": sum(row["total"] for row in stacked),
        "connector_total": sum(row["count"] for row in connectors),
    }


@app.get("/api/dashboard")
async def dashboard():
    session = _Session()
    try:
        return dashboard_payload(session)
    finally:
        session.close()


_LIST_STATUSES = frozenset({"rejected", "expired", "archived"})
_LIST_DEFER_COLS = (
    Job.description,
    Job.description_text,
    Job.cover_letter,
    Job.fit_explanation,
    Job.llm_strengths,
    Job.skill_gaps,
)


@app.get("/api/jobs")
async def list_jobs(status: str = "review", limit: Optional[int] = None):
    session = _Session()
    try:
        if status == "archived":
            status_filter = Job.archived.is_(True)
        else:
            status_filter = (Job.status == status) & _not_archived()
        query = (
            session.query(Job)
            .filter(status_filter)
            .options(*(defer(col) for col in _LIST_DEFER_COLS))
        )
        if status in ("shortlisted", "review"):
            display_score = func.coalesce(Job.llm_fit_score, Job.fit_score)
            query = query.order_by(
                func.date(Job.created_at).desc().nullslast(),
                display_score.desc().nullslast(),
                Job.id.desc(),
            )
        elif status == "applied":
            query = query.order_by(
                Job.updated_at.desc().nullslast(),
                Job.created_at.desc().nullslast(),
                Job.id.desc(),
            )
        else:
            query = query.order_by(Job.fit_score.desc().nullslast(), Job.id.desc())
        cap = limit
        if cap is None:
            cap = 0 if status in _LIST_STATUSES or status in ("shortlisted", "applied") else 200
        if cap and cap > 0:
            query = query.limit(cap)
        jobs = query.all()
        sites = _company_website_map(session, [j.company or "" for j in jobs])
        applied_keys = recent_applied_company_keys(session)
        rows = [
            _job_to_dict(
                j,
                sites.get(company_name_key(j.company or ""), ""),
                include_body=False,
                applied_company_keys=applied_keys,
            )
            for j in jobs
        ]
        payload: Dict[str, Any] = {
            "jobs": rows,
            "total": len(jobs),
        }
        if status == "rejected":
            rows = (
                session.query(Job.reject_code, func.count(Job.id))
                .filter(Job.status == "rejected", _not_archived())
                .group_by(Job.reject_code)
                .all()
            )
            payload["reject_counts"] = [
                {
                    "code": code or "unknown",
                    "label": REJECT_LABELS.get(code or "", "Unknown"),
                    "count": n,
                }
                for code, n in sorted(rows, key=lambda r: (-r[1], r[0] or ""))
            ]
        return payload
    finally:
        session.close()


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        if (job.source or "") == "waas":
            current = job.description_text or job.description or ""
            if len(current) < 800:
                from connectors.waas import hydrate_job_from_public_page
                refreshed = hydrate_job_from_public_page(job.url or "")
                if refreshed:
                    cleaned = clean_description(refreshed)
                    if len(cleaned) > len(current):
                        job.description = refreshed
                        job.description_text = cleaned
                        session.commit()
        return _job_to_dict_with_site(session, job)
    finally:
        session.close()


_JOB_STATUSES = frozenset({
    "new", "review", "shortlisted", "applied",
    "deferred", "rejected", "expired", "archived",
})


def _apply_job_status(job: Job, new_status: str) -> bool:
    """Set status and the reject-reason side effects. False when unchanged."""
    previous = job.status
    if previous == new_status:
        return False
    job.status = new_status
    if new_status == "rejected" and previous != "rejected" and not _plain_str(job.reject_code):
        job.reject_code = "manual"
        job.reject_detail = "Rejected from triage"
    elif new_status in ("review", "new") and previous == "rejected":
        job.reject_code = None
        job.reject_detail = None
    return True


class StatusUpdate(BaseModel):
    status: str


@app.post("/api/jobs/{job_id}/status")
async def update_status(job_id: int, body: StatusUpdate):
    allowed = {"shortlisted", "rejected", "deferred", "review", "applied", "expired", "archived"}
    if body.status not in allowed:
        raise HTTPException(400, f"Invalid status: {body.status}")
    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        if body.status == "archived":
            job.archived = True
        else:
            _apply_job_status(job, body.status)
            job.archived = False
        session.commit()
        # Close the Playwright browser when the user marks a job as applied,
        # so they can immediately open the next job without hitting the
        # "prefill already running" guard.
        if body.status in ("applied", "rejected"):
            with _prefill_lock:
                if _prefill["status"] == "running" and _prefill["job_id"] == job_id:
                    _prefill_cancel.set()
        return {
            "ok": True,
            "id": job_id,
            "status": job.status,
            "archived": bool(job.archived),
        }
    except HTTPException:
        raise
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


class BulkStatusRequest(BaseModel):
    ids: List[int]
    status: str


class ArchiveUpdate(BaseModel):
    archived: bool = True


@app.post("/api/jobs/{job_id}/archive")
async def set_archived(job_id: int, body: ArchiveUpdate):
    """Hide or restore a job without changing its status bucket."""
    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        job.archived = body.archived
        session.commit()
        return {
            "ok": True,
            "id": job_id,
            "status": job.status,
            "archived": bool(job.archived),
        }
    except HTTPException:
        raise
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


def _bulk_status_values(new_status: str) -> dict:
    """Column updates for one SQL statement. Matches `_apply_job_status`."""
    values = {"status": new_status, "updated_at": datetime.now(timezone.utc)}
    if new_status == "rejected":
        blank = or_(Job.reject_code.is_(None), Job.reject_code == "")
        values["reject_code"] = case((blank, "manual"), else_=Job.reject_code)
        values["reject_detail"] = case((blank, "Rejected from triage"), else_=Job.reject_detail)
    elif new_status in ("review", "new"):
        clear = Job.status == "rejected"
        values["reject_code"] = case((clear, None), else_=Job.reject_code)
        values["reject_detail"] = case((clear, None), else_=Job.reject_detail)
    values["archived"] = False
    return values


@app.post("/api/jobs/bulk-status")
async def bulk_status(body: BulkStatusRequest):
    if body.status not in _JOB_STATUSES:
        raise HTTPException(400, f"Invalid status: {body.status}")
    ids = list(dict.fromkeys(i for i in body.ids if isinstance(i, int)))
    if not ids:
        raise HTTPException(400, "No jobs selected")
    session = _Session()
    try:
        updated = 0
        if body.status == "archived":
            values = {"archived": True, "updated_at": datetime.now(timezone.utc)}
            changed = or_(Job.archived.is_(False), Job.archived.is_(None))
        else:
            values = _bulk_status_values(body.status)
            changed = or_(Job.status != body.status, Job.archived.is_(True))
        for start in range(0, len(ids), 500):
            stmt = (
                update(Job)
                .where(Job.id.in_(ids[start:start + 500]), changed)
                .values(**values)
                .execution_options(synchronize_session=False)
            )
            result = session.execute(stmt)
            updated += max(result.rowcount or 0, 0)
        session.commit()
        return {"ok": True, "updated": updated, "status": body.status}
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


class BulkRejectStaleRequest(BaseModel):
    status: str
    older_than_days: Optional[int] = None


@app.post("/api/jobs/bulk-archive-stale")
async def bulk_archive_stale(body: BulkRejectStaleRequest):
    allowed = {"shortlisted", "review"}
    if body.status not in allowed:
        raise HTTPException(400, f"bulk-archive-stale only supports: {', '.join(sorted(allowed))}")
    days = config.REJECT_STALE_DAYS if body.older_than_days is None else body.older_than_days
    if days < 1:
        raise HTTPException(400, "older_than_days must be at least 1")
    cutoff = datetime.utcnow() - timedelta(days=days)
    session = _Session()
    try:
        age_on = func.coalesce(Job.posted_date, Job.created_at)
        stale = (
            session.query(Job)
            .filter(Job.status == body.status, _not_archived(), age_on < cutoff)
            .all()
        )
        count = len(stale)
        for job in stale:
            job.archived = True
        session.commit()
        return {"ok": True, "archived": count}
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


@app.post("/api/jobs/{job_id}/explain")
async def explain_job(job_id: int):
    """Run a one-job LLM analysis without changing status."""
    from utils.llm_analysis import analyze_job_with_ollama

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        try:
            with open("profile.yaml", encoding="utf-8") as f:
                profile = yaml.safe_load(f) or {}
        except Exception:
            profile = {}
        job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
        analysis = analyze_job_with_ollama(job_dict, profile, config.OLLAMA_MODEL)
        if analysis.get("llm_status") == "failed":
            raise HTTPException(500, analysis.get("error") or "LLM analysis failed")
        job.llm_fit_score = analysis.get("llm_fit_score")
        job.llm_strengths = json.dumps(analysis.get("llm_strengths", []), ensure_ascii=False)
        job.fit_explanation = analysis.get("fit_explanation")
        job.skill_gaps = json.dumps(analysis.get("skill_gaps", []), ensure_ascii=False)
        job.recommendation = analysis.get("recommendation")
        job.llm_confidence = analysis.get("llm_confidence")
        job.llm_status = analysis.get("llm_status")
        if analysis.get("recommended_resume"):
            job.recommended_resume = analysis.get("recommended_resume")
        session.commit()
        session.refresh(job)
        return {"ok": True, "job": _job_to_dict_with_site(session, job)}
    except HTTPException:
        raise
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


@app.post("/api/jobs/{job_id}/cover-letter")
async def generate_cover(job_id: int):
    from utils.cover_letter import generate_cover_letter
    import yaml

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")

        try:
            with open("profile.yaml", encoding="utf-8") as f:
                profile = yaml.safe_load(f) or {}
        except Exception:
            profile = {}

        result = generate_cover_letter(_job_to_dict(job), profile)
        if result["status"] == "ok":
            job.cover_letter = result["cover_letter"]
            session.commit()
            return {"ok": True, "cover_letter": result["cover_letter"]}
        else:
            raise HTTPException(500, result.get("error", "Generation failed"))
    except HTTPException:
        raise
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


@app.get("/api/jobs/{job_id}/cover-letter/pdf")
async def download_cover_pdf(job_id: int):
    """Return the cover letter for job_id as a downloadable PDF."""
    import io
    import re as _re
    from fastapi.responses import StreamingResponse

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        text = (job.cover_letter or "").strip()
        if not text:
            raise HTTPException(404, "No cover letter for this job")

        company = (job.company or "company").strip()
        title = (job.title or "position").strip()

        try:
            from fpdf import FPDF
        except ImportError as exc:
            raise HTTPException(500, f"fpdf2 not installed: {exc}") from exc

        def _to_latin1(s: str) -> str:
            """Map common Unicode typographic chars to Latin-1 equivalents."""
            _MAP = str.maketrans({
                "\u2018": "'", "\u2019": "'",   # left/right single quotes
                "\u201c": '"', "\u201d": '"',   # left/right double quotes
                "\u2013": "-", "\u2014": "-",   # en/em dash
                "\u2026": "...",                 # ellipsis
                "\u00a0": " ",                   # non-breaking space
            })
            return s.translate(_MAP).encode("latin-1", errors="replace").decode("latin-1")

        pdf = FPDF()
        pdf.set_margins(25, 25, 25)
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 13)
        from fpdf.enums import XPos, YPos
        pdf.cell(0, 8, _to_latin1(f"{title} - {company}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(4)
        pdf.set_font("Helvetica", size=11)
        for para in text.split("\n\n"):
            para = para.strip()
            if not para:
                continue
            pdf.multi_cell(0, 6, _to_latin1(para))
            pdf.ln(3)

        buf = io.BytesIO(pdf.output())
        slug = _re.sub(r"[^\w-]", "_", company.lower())[:40]
        filename = f"cover_letter_{slug}.pdf"
        return StreamingResponse(
            buf,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        raise
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Prefill state
# ---------------------------------------------------------------------------

_prefill: Dict[str, Any] = {
    "status": "idle",
    "job_id": None,
    "result": None,
    "log": [],
    "started_at": None,
    "cover_letter": None,
}
_prefill_lock = threading.Lock()
_prefill_cancel = threading.Event()


def _prefill_log(msg: str) -> None:
    """Append a timestamped message to the prefill log (thread-safe)."""
    entry = f"{datetime.utcnow().strftime('%H:%M:%S')} {msg}"
    with _prefill_lock:
        _prefill["log"].append(entry)
    print(entry, flush=True)  # also echo to terminal


def _cancel_existing_prefill() -> None:
    """Signal any running prefill session to stop and reset state.

    Does not wait for the browser thread to exit — the cancel event is
    enough for run_prefill_session to close the browser on its next await.
    """
    _prefill_cancel.set()
    with _prefill_lock:
        _prefill["status"] = "idle"
        _prefill["job_id"] = None
        _prefill["result"] = None
        _prefill["log"] = []
        _prefill["cover_letter"] = None


def _scrape_job_meta(url: str) -> Dict[str, Any]:
    """Fetch *url* and extract job title, company, and description.

    Tries in order:
    1. JSON-LD ``JobPosting`` schema (most reliable, used by Greenhouse,
       Lever, Ashby, and most modern ATS pages for SEO).
    2. OpenGraph ``og:title`` / ``og:site_name`` / ``og:description`` meta tags.
    3. ``<title>`` tag — parsed on common separators (`` | ``, `` - ``, `` · ``).

    Returns a dict with keys ``title``, ``company``, ``description``
    (all strings, any may be empty if detection failed).
    """
    import requests as _req
    try:
        resp = _req.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; job-apply-agent/1.0)"}, timeout=10)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        return {}

    # --- 1. JSON-LD JobPosting ---
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(m.group(1).strip())
        except Exception:
            continue
        if data.get("@type") == "JobPosting":
            title = (data.get("title") or "").strip()
            company = ((data.get("hiringOrganization") or {}).get("name") or "").strip()
            description = (data.get("description") or "").strip()
            if title or company:
                return {"title": title, "company": company, "description": description}

    # --- 2. OpenGraph meta tags ---
    def _meta(prop: str) -> str:
        m = re.search(
            r'<meta[^>]+(?:property|name)=["\']' + re.escape(prop) + r'["\'][^>]+content=["\'](.*?)["\']',
            html, re.IGNORECASE,
        )
        if not m:
            # Also handle content= before property=
            m = re.search(
                r'<meta[^>]+content=["\'](.*?)["\'][^>]+(?:property|name)=["\']' + re.escape(prop) + r'["\']',
                html, re.IGNORECASE,
            )
        return m.group(1).strip() if m else ""

    og_title = _meta("og:title")
    og_site  = _meta("og:site_name")
    og_desc  = _meta("og:description")

    # --- 3. <title> tag fallback ---
    page_title_m = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    page_title = re.sub(r'<[^>]+>', '', page_title_m.group(1)).strip() if page_title_m else ""

    inferred_title = og_title
    inferred_company = og_site

    if not inferred_title and page_title:
        for sep in (" | ", " - ", " · ", " — "):
            parts = [p.strip() for p in page_title.split(sep) if p.strip()]
            if len(parts) >= 2:
                inferred_title = parts[0]
                if not inferred_company:
                    inferred_company = parts[1]
                break
        if not inferred_title:
            inferred_title = page_title

    return {
        "title": inferred_title,
        "company": inferred_company,
        "description": og_desc,
    }


def _persist_prefill_cover_letter(job_id: Any, text: str) -> bool:
    """Save Open & Apply cover letter text onto the job row."""
    if not job_id or not (text or "").strip():
        return False
    session = _Session()
    try:
        db_job = session.query(Job).filter(Job.id == job_id).first()
        if not db_job:
            return False
        db_job.cover_letter = text
        session.commit()
        return True
    except Exception as exc:
        session.rollback()
        _prefill_log(f"Cover letter save failed: {exc}")
        return False
    finally:
        session.close()


def _run_prefill_thread(job_dict: Dict[str, Any], profile: Dict[str, Any]) -> None:
    with _prefill_lock:
        _prefill["log"] = []
        _prefill["started_at"] = datetime.utcnow().isoformat()
        _prefill["status"] = "running"  # "starting" → "running" so the UI poll keeps going
        _prefill["cover_letter"] = job_dict.get("cover_letter") or None

    _prefill_log(f"Starting prefill for: {job_dict.get('title', '')} @ {job_dict.get('company', '')}")

    # For direct URL fills, scrape the page to enrich missing title / company /
    # description before the cover letter is generated.
    if job_dict.get("source") == "direct" and (
        not job_dict.get("company")
        or not job_dict.get("title")
        or job_dict.get("title") == "Direct Fill"
        or not job_dict.get("description_text")
    ):
        _prefill_log("Scraping job metadata from URL…")
        try:
            meta = _scrape_job_meta(job_dict["url"])
            if meta:
                job_dict = dict(job_dict)
                if meta.get("title") and (
                    not job_dict.get("title") or job_dict.get("title") == "Direct Fill"
                ):
                    job_dict["title"] = meta["title"]
                if meta.get("company") and not job_dict.get("company"):
                    job_dict["company"] = meta["company"]
                if meta.get("description") and not job_dict.get("description_text"):
                    job_dict["description_text"] = meta["description"]
                _prefill_log(
                    f"Metadata: title={job_dict.get('title')!r}  "
                    f"company={job_dict.get('company')!r}"
                )
        except Exception as exc:
            _prefill_log(f"Metadata scrape failed: {exc}")

    if job_dict.get("cover_letter"):
        cl_text = job_dict.get("cover_letter", "")
        preview = cl_text[:120].replace("\n", " ")
        _prefill_log(f"Cover letter ready: {preview}…")

    def _gen_cover_letter(job: Dict[str, Any], profile: Dict[str, Any]) -> None:
        """Generate, persist, and write a cover letter PDF.

        Called on-demand from the browser session only when the form actually
        has a cover letter field — avoids wasting LLM time when the application
        link is dead or the form has no cover letter field.
        """
        from utils.cover_letter import generate_cover_letter
        _prefill_log("Cover letter field detected — generating via LLM…")
        try:
            cl_result = generate_cover_letter(job, profile)
            if cl_result.get("status") == "ok":
                letter = cl_result["cover_letter"]
                job["cover_letter"] = letter
                with _prefill_lock:
                    _prefill["cover_letter"] = letter
                preview = letter[:120].replace("\n", " ")
                _prefill_log(f"Cover letter generated: {preview}…")
                if _persist_prefill_cover_letter(job.get("id"), letter):
                    _prefill_log("Cover letter saved — it will appear on the Cover Letter tab.")
                # Write PDF so the user can also upload it manually.
                try:
                    from utils.form_filler import _resolve_cover_letter_path
                    _resolve_cover_letter_path({}, job, log_fn=_prefill_log)
                except Exception as exc:
                    _prefill_log(f"Cover letter PDF save error: {exc}")
            else:
                _prefill_log("Cover letter generation failed — will skip upload.")
        except Exception as exc:
            _prefill_log(f"Cover letter error: {exc}")

    _prefill_log("Launching browser…")
    from utils.form_prefill import run_prefill_session

    try:
        result = asyncio.run(run_prefill_session(job_dict, profile, cancel_event=_prefill_cancel, log_fn=_prefill_log, timing=__import__("os").environ.get("CC_PROFILE") == "1", cover_letter_fn=_gen_cover_letter))
    except Exception as exc:
        result = {"status": "failed", "error": str(exc)}

    st = result.get("status", "")
    if st == "failed":
        _prefill_log(f"Prefill failed: {result.get('error', 'unknown error')}")
    elif st == "manual":
        _prefill_log(f"Manual: {result.get('reason', 'open in system browser')}")
    elif st == "cancelled":
        _prefill_log("Prefill stopped by user.")
    else:
        filled = result.get("filled", 0)
        skipped = result.get("skipped", 0)
        errors = result.get("errors", 0)
        uploads = result.get("uploads", 0)
        ats = result.get("ats", "unknown")
        _prefill_log(f"Session done ({ats}): {filled} filled, {uploads} uploaded, {skipped} skipped, {errors} errors.")

    with _prefill_lock:
        _prefill["status"] = "done"
        _prefill["result"] = result


# ---------------------------------------------------------------------------
# Routes — pipeline
# ---------------------------------------------------------------------------

@app.get("/api/pipeline/status")
async def pipeline_status():
    with _pipeline_lock:
        return dict(_pipeline)


@app.post("/api/pipeline/run")
async def pipeline_run():
    with _pipeline_lock:
        if _pipeline["status"] == "running":
            return {"ok": False, "message": "Pipeline already running"}

    thread = threading.Thread(target=_run_pipeline_subprocess, daemon=True)
    thread.start()
    return {"ok": True, "message": "Pipeline started"}


# ---------------------------------------------------------------------------
# Routes — schedule
# ---------------------------------------------------------------------------

class ScheduleConfig(BaseModel):
    mode: str
    interval_hours: int = 4
    times: List[str] = []


@app.get("/api/schedule")
async def get_schedule():
    return {
        **_sched_config,
        "next_run": _next_run_iso(),
        "task_scheduler": _read_task_scheduler(),
    }


@app.post("/api/schedule")
async def set_schedule(body: ScheduleConfig):
    global _sched_config
    if body.mode not in {"off", "interval", "daily"}:
        raise HTTPException(400, f"Invalid mode: {body.mode}")
    _sched_config = {"mode": body.mode, "interval_hours": body.interval_hours, "times": body.times}
    _apply_schedule()
    _save_sched_config()
    return {"ok": True, "next_run": _next_run_iso(), **_sched_config}


class FullRunSourcesUpdate(BaseModel):
    enabled: List[str]


@app.get("/api/settings/connectors")
async def get_full_run_connectors():
    from run_pipeline import CONNECTORS
    from utils.full_run_sources import read_enabled

    saved = set(read_enabled() or [])
    return {
        "connectors": [
            {"name": name, "enabled": name in saved}
            for name in CONNECTORS
        ]
    }


@app.post("/api/settings/connectors")
async def set_full_run_connectors(body: FullRunSourcesUpdate):
    from run_pipeline import CONNECTORS
    from utils.full_run_sources import save_enabled

    unknown = [name for name in body.enabled if name not in CONNECTORS]
    if unknown:
        raise HTTPException(400, f"Unknown connectors: {', '.join(unknown)}")
    chosen = set(body.enabled)
    ordered = [name for name in CONNECTORS if name in chosen]
    save_enabled(ordered)
    return {"ok": True, "enabled": ordered}


# ---------------------------------------------------------------------------
# Routes — open / prefill
# ---------------------------------------------------------------------------

@app.post("/api/jobs/{job_id}/open")
async def open_job(job_id: int):
    from utils.form_prefill import is_system_browser_domain

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        job_dict = _job_to_dict(job)
    finally:
        session.close()

    # Bot-protected domains: tell the UI to open in system browser directly.
    if is_system_browser_domain(job_dict.get("url", "")):
        return {"ok": True, "system_browser": True, "url": job_dict.get("url", "")}

    # Cancel any existing session before starting a new one.
    _cancel_existing_prefill()
    _prefill_cancel.clear()

    with _prefill_lock:
        _prefill["status"] = "starting"
        _prefill["job_id"] = job_id
        _prefill["result"] = None
        _prefill["log"] = []
        _prefill["cover_letter"] = None

    try:
        with open("profile.yaml", encoding="utf-8") as fh:
            profile = yaml.safe_load(fh) or {}
    except Exception:
        profile = {}

    threading.Thread(target=_run_prefill_thread, args=(job_dict, profile), daemon=True).start()
    return {"ok": True, "system_browser": False, "message": "Browser opening…"}


@app.get("/api/prefill/status")
async def get_prefill_status():
    with _prefill_lock:
        return dict(_prefill)


@app.post("/api/prefill/stop")
async def stop_prefill():
    _prefill_cancel.set()
    # Check status without holding the lock when logging — _prefill_log itself
    # acquires _prefill_lock, so calling it inside another with _prefill_lock
    # would deadlock (threading.Lock is not reentrant).
    with _prefill_lock:
        was_running = _prefill["status"] == "running"
    if was_running:
        _prefill_log("Stop requested by user.")
    return {"ok": True}


class DirectFillRequest(BaseModel):
    url: str
    title: str = ""
    company: str = ""
    description: str = ""


@app.post("/api/prefill/url")
async def prefill_url(body: DirectFillRequest):
    """Open a browser directly on the provided application URL and fill the form.

    Use this when the real application form URL is known but is behind auth or
    bot-detection that prevents the normal pipeline from reaching it.
    The browser opens at the URL; if a login is still required the user can
    complete it manually before the form is auto-filled.
    """
    if not body.url or not body.url.startswith("http"):
        raise HTTPException(400, "A valid http(s) URL is required")

    # Cancel any existing session before starting a new one.
    _cancel_existing_prefill()
    _prefill_cancel.clear()

    with _prefill_lock:
        _prefill["status"] = "running"
        _prefill["job_id"] = None
        _prefill["result"] = None
        _prefill["log"] = []
        _prefill["cover_letter"] = None

    try:
        with open("profile.yaml", encoding="utf-8") as fh:
            profile = yaml.safe_load(fh) or {}
    except Exception:
        profile = {}

    # Build a synthetic job dict — no DB record needed.
    job_dict = {
        "id": None,
        "url": body.url,
        "title": body.title or "Direct Fill",
        "company": body.company or "",
        "description_text": body.description or "",
        "source": "direct",
        "status": "shortlisted",
        "cover_letter": None,
    }

    threading.Thread(target=_run_prefill_thread, args=(job_dict, profile), daemon=True).start()
    return {"ok": True, "message": "Browser opening…"}


# ---------------------------------------------------------------------------
# Interview prep
# ---------------------------------------------------------------------------

_prep_running: Dict[int, bool] = {}
_prep_lock = threading.Lock()


def _prep_sheet_to_dict(sheet: InterviewPrepSheet) -> Dict[str, Any]:
    def _j(raw):
        if not raw:
            return None
        try:
            import json as _json
            return _json.loads(raw)
        except Exception:
            return raw

    return {
        "id": sheet.id,
        "job_application_id": sheet.job_application_id,
        "status": sheet.status,
        "company_snapshot": _j(sheet.company_snapshot),
        "role_requirements_summary": _j(sheet.role_requirements_summary),
        "likely_technical_questions": _j(sheet.likely_technical_questions),
        "likely_behavioral_questions": _j(sheet.likely_behavioral_questions),
        "talking_points": _j(sheet.talking_points),
        "gaps_or_risks": _j(sheet.gaps_or_risks),
        "prep_plan_30_min": _j(sheet.prep_plan_30_min),
        "error_message": sheet.error_message,
        "generated_at": sheet.generated_at.isoformat() if sheet.generated_at else None,
    }


def _run_prep_thread(job_id: int) -> None:
    from utils.interview_prep import run_interview_prep

    session = _Session()
    try:
        with open("profile.yaml", encoding="utf-8") as fh:
            profile = yaml.safe_load(fh) or {}
    except Exception:
        profile = {}

    try:
        run_interview_prep(job_id, profile, session)
    except Exception:
        pass
    finally:
        session.close()
        with _prep_lock:
            _prep_running.pop(job_id, None)


@app.get("/api/jobs/{job_id}/interview-prep")
async def get_interview_prep(job_id: int):
    session = _Session()
    try:
        sheet = session.query(InterviewPrepSheet).filter(
            InterviewPrepSheet.job_application_id == job_id
        ).first()
        if not sheet:
            with _prep_lock:
                running = _prep_running.get(job_id, False)
            return {"found": False, "running": running}
        return {"found": True, "running": False, "sheet": _prep_sheet_to_dict(sheet)}
    finally:
        session.close()


@app.post("/api/jobs/{job_id}/interview-prep")
async def generate_interview_prep(job_id: int):
    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
    finally:
        session.close()

    with _prep_lock:
        if _prep_running.get(job_id):
            return {"ok": True, "message": "Already generating"}
        _prep_running[job_id] = True

    threading.Thread(target=_run_prep_thread, args=(job_id,), daemon=True).start()
    return {"ok": True, "message": "Generation started"}


# ---------------------------------------------------------------------------
# Company research (cached per normalized company name)
# ---------------------------------------------------------------------------

_company_running: Dict[str, bool] = {}
_company_lock = threading.Lock()


def _parse_json_field(raw: Any) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _company_profile_to_dict(row: CompanyProfile) -> Dict[str, Any]:
    return {
        "id": row.id,
        "name_key": row.name_key,
        "display_name": row.display_name or "",
        "website_url": row.website_url or "",
        "website_host": row.website_host or "",
        "status": row.status,
        "analysis": _parse_json_field(row.analysis) or {},
        "sources": _parse_json_field(row.sources) or [],
        "error_message": row.error_message,
        "generated_at": row.generated_at.isoformat() if row.generated_at else None,
    }


def _run_company_thread(job_id: int, name_key: str, regenerate: bool) -> None:
    from utils.company_research import run_company_research

    session = _Session()
    try:
        with open("profile.yaml", encoding="utf-8") as fh:
            profile = yaml.safe_load(fh) or {}
    except Exception:
        profile = {}
    try:
        run_company_research(job_id, profile, session, regenerate=regenerate)
    except Exception:
        pass
    finally:
        session.close()
        with _company_lock:
            _company_running.pop(name_key, None)


@app.get("/api/jobs/{job_id}/company")
async def get_company_profile(job_id: int):
    from utils.company_research import find_profile

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        key = company_name_key(job.company or "")
        row = find_profile(session, job.company or "")
        with _company_lock:
            running = bool(key and _company_running.get(key))
        if not row:
            return {"found": False, "running": running}
        return {
            "found": True,
            "running": running,
            "profile": _company_profile_to_dict(row),
        }
    finally:
        session.close()


@app.post("/api/jobs/{job_id}/company")
async def generate_company_profile(job_id: int, regenerate: bool = False):
    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        key = company_name_key(job.company or "")
        if not key:
            raise HTTPException(400, "Job has no company name")
        if not regenerate:
            from utils.company_research import find_profile

            existing = find_profile(session, job.company or "")
            if existing and existing.status == "completed":
                return {"ok": True, "message": "Already cached", "cached": True}
    finally:
        session.close()

    with _company_lock:
        if _company_running.get(key):
            return {"ok": True, "message": "Already generating"}
        _company_running[key] = True

    threading.Thread(
        target=_run_company_thread, args=(job_id, key, regenerate), daemon=True
    ).start()
    return {"ok": True, "message": "Generation started"}


# ---------------------------------------------------------------------------
# Resume → profile.yaml parser
# ---------------------------------------------------------------------------

@app.post("/api/profile/parse-resume")
async def parse_resume_endpoint(file: UploadFile = File(...)):
    """Accept a PDF resume upload and return a profile.yaml string.

    The returned YAML is merged into the existing profile.yaml on disk
    (structural sections like credentials, target_companies, etc. are preserved).
    The merged result is also written back to profile.yaml.
    """
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are accepted")

    import tempfile
    import os
    from utils.resume_parser import parse_resume_to_yaml

    # Save upload to a temp file
    suffix = ".pdf"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        # Load existing profile to preserve structural sections
        existing: dict = {}
        try:
            with open("profile.yaml", encoding="utf-8") as fh:
                existing = yaml.safe_load(fh) or {}
        except Exception:
            pass

        result_yaml, was_reparsed = parse_resume_to_yaml(tmp_path, existing_profile=existing)

        if not was_reparsed:
            return {"ok": True, "unchanged": True,
                    "message": "Resume unchanged — profile.yaml not modified"}

        # Write merged result back to profile.yaml
        try:
            with open("profile.yaml", "w", encoding="utf-8") as fh:
                fh.write(result_yaml)
        except Exception as write_err:
            return {"ok": False, "error": f"Could not write profile.yaml: {write_err}",
                    "yaml": result_yaml}

        return {"ok": True, "unchanged": False, "yaml": result_yaml}
    except Exception as exc:
        raise HTTPException(500, str(exc))
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
