import os
import sys
import asyncio
import click

# Force UTF-8 output on Windows so Unicode chars (e.g. UTC −08:00) don't crash.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)  # type: ignore
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)  # type: ignore
import yaml
import datetime
import json
import utils.ssl_compat  # noqa: F401  — trust OS CAs for requests HTTPS
from sqlalchemy import create_engine, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from models.database import Job, PipelineRun, ApplicationHistory, ensure_company_profiles, ensure_job_columns
from utils.job_store import configure_sqlite
from connectors.remotive import RemotiveConnector
from connectors.remoteok import RemoteOKConnector
from connectors.weworkremotely import WeWorkRemotelyConnector
from connectors.arbeitnow import ArbeitnowConnector
from connectors.jobicy import JobicyConnector
from connectors.jobspresso import JobspressoConnector
from connectors.dynamitejobs import DynamiteJobsConnector
from connectors.workingnomads import WorkingNomadsConnector
from connectors.getonboard import GetOnBoardConnector
from connectors.himalayas import HimalayasConnector
from connectors.adzuna import AdzunaConnector
from connectors.ashby import AshbyConnector
from connectors.greenhouse import GreenhouseConnector
from connectors.lever import LeverConnector
from connectors.direct_ats import DirectATSConnector
from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
from connectors.euremotejobs import EURemoteJobsConnector
from connectors.remoteaijobs import RemoteAIJobsConnector
from connectors.nodesk import NodeskConnector
from connectors.remote100k import Remote100kConnector
from connectors.wearedistributed import WeAreDistributedConnector
from connectors.flexa import FlexaConnector
from connectors.remotejobsio import RemoteJobsIoConnector
from connectors.remotejobsfinder import RemoteJobsFinderConnector
from connectors.dailyremote import DailyRemoteConnector
from connectors.arcdev import ArcDevConnector
from connectors.flexjobs import FlexJobsConnector
from connectors.ycombinator import YCombinatorConnector
from connectors.waas import WaasConnector
from connectors.techjobsforgood import TechJobsForGoodConnector
from connectors.remotecom import RemoteComConnector
from connectors.remoteco import RemoteCoConnector
from connectors.devremote import DevRemoteConnector
from connectors.wearedevelopers import WeAreDevelopersConnector
from connectors.anywherepositions import AnywherePositionsConnector
from connectors.remoterocketship import RemoteRocketshipConnector
from connectors.dice import DiceConnector
from connectors.workable import WorkableConnector
from connectors.remotescout24 import RemoteScout24Connector
from connectors.trulyremote import TrulyRemoteConnector
from connectors.aijobs import AIJobsConnector
from connectors.aijobsai import AIJobsAIConnector
from connectors.justjoin import JustJoinConnector
from connectors.brenxor import BrenxorConnector
from connectors.jobgether import JobgetherConnector
from connectors.postjobfree import PostJobFreeConnector
from connectors.topsalaries import TopSalariesConnector
from connectors.levelsfyi import LevelsFyiConnector
from connectors.workew import WorkewConnector
from connectors.ladders import LaddersConnector
from connectors.startupjobs import StartupJobsConnector
from connectors.fourdayweek import FourDayWeekConnector
from connectors.builtin import BuiltinConnector
from connectors.virtualvocations import VirtualVocationsConnector
from connectors.up2staff import Up2StaffConnector
from connectors.remotearmy import RemoteArmyConnector
from connectors.remoteyeah import RemoteYeahConnector
from connectors.remotesource import RemoteSourceConnector
from connectors.remotejobs import RemoteJobsConnector
from connectors.remotefrontjobs import RemoteFrontJobsConnector
from connectors.remotefront import RemoteFrontConnector
from connectors.remotewlb import RemoteWlbConnector
from connectors.omnijobs import OmniJobsConnector
from connectors.techcareers import TechCareersConnector
from connectors.smartrecruiters import SmartRecruitersConnector
from connectors.hubstafftalent import HubstaffTalentConnector
from connectors.tryremotely import TryRemotelyConnector
from connectors.findmyremote import FindMyRemoteConnector
from connectors.jobdiva import JobDivaConnector
from utils.form_prefill import _TimingCollector
from utils.dedup import backfill_dedup_keys, collapse_duplicate_jobs, is_duplicate
from utils.application_filter import has_already_applied
from utils.llm_analysis import analyze_job_with_ollama, ollama_is_reachable
from utils.ollama_client import request_headers, unreachable_hint
from utils.scoring import SHORTLIST_MIN_SCORE, dump_score_breakdown, score_job
from utils.resume_selector import select_resume
from utils.logger import setup_logger
from utils.email_report import send_report
from utils.full_run_sources import enabled_sources, read_enabled
from utils.alerts import ScrapeHalted, alert, halt, is_db_write_error, is_skippable_db_lock
from utils.job_age import age_days_override, max_job_age_days, resolve_job_age_days
from utils.mongo_jobs import mirror_job
from utils.job_inclusion import drop_ineligible_jobs, exclusion_reason, load_candidate_profile
import config

logger = setup_logger("run_pipeline")

CONNECTORS = {
    "remotive": RemotiveConnector,
    "remoteok": RemoteOKConnector,
    "weworkremotely": WeWorkRemotelyConnector,
    "arbeitnow": ArbeitnowConnector,
    "jobicy": JobicyConnector,
    "jobspresso": JobspressoConnector,
    "dynamitejobs": DynamiteJobsConnector,
    "workingnomads": WorkingNomadsConnector,
    "getonboard": GetOnBoardConnector,
    "himalayas": HimalayasConnector,
    "adzuna": AdzunaConnector,
    "realworkfromanywhere": RealWorkFromAnywhereConnector,
    "euremotejobs": EURemoteJobsConnector,
    "remoteaijobs": RemoteAIJobsConnector,
    "nodesk": NodeskConnector,
    "remote100k": Remote100kConnector,
    "wearedistributed": WeAreDistributedConnector,
    "flexa": FlexaConnector,
    "remotejobsio": RemoteJobsIoConnector,
    "remotejobsfinder": RemoteJobsFinderConnector,
    "dailyremote": DailyRemoteConnector,
    "arcdev": ArcDevConnector,
    "flexjobs": FlexJobsConnector,
    "ycombinator": YCombinatorConnector,
    "waas": WaasConnector,
    "techjobsforgood": TechJobsForGoodConnector,
    "remotecom": RemoteComConnector,
    "remoteco": RemoteCoConnector,
    "devremote": DevRemoteConnector,
    "anywherepositions": AnywherePositionsConnector,
    "remoterocketship": RemoteRocketshipConnector,
    "dice": DiceConnector,
    "workable": WorkableConnector,
    "remotescout24": RemoteScout24Connector,
    "trulyremote": TrulyRemoteConnector,
    "aijobs": AIJobsConnector,
    "aijobsai": AIJobsAIConnector,
    "justjoin": JustJoinConnector,
    "brenxor": BrenxorConnector,
    "jobgether": JobgetherConnector,
    "postjobfree": PostJobFreeConnector,
    "topsalaries": TopSalariesConnector,
    "levelsfyi": LevelsFyiConnector,
    "workew": WorkewConnector,
    "ladders": LaddersConnector,
    "startupjobs": StartupJobsConnector,
    "4dayweek": FourDayWeekConnector,
    "builtin": BuiltinConnector,
    "virtualvocations": VirtualVocationsConnector,
    "up2staff": Up2StaffConnector,
    "remotearmy": RemoteArmyConnector,
    "remoteyeah": RemoteYeahConnector,
    "remotesource": RemoteSourceConnector,
    "remotejobs": RemoteJobsConnector,
    "remotefrontjobs": RemoteFrontJobsConnector,
    "remotefront": RemoteFrontConnector,
    "remotewlb": RemoteWlbConnector,
    "omnijobs": OmniJobsConnector,
    "techcareers": TechCareersConnector,
    "smartrecruiters": SmartRecruitersConnector,
    "hubstafftalent": HubstaffTalentConnector,
    "tryremotely": TryRemotelyConnector,
    "findmyremote": FindMyRemoteConnector,
    "jobdiva": JobDivaConnector,
    # After aggregators so board slugs come from this run's ATS apply URLs.
    "ashby": AshbyConnector,
    "greenhouse": GreenhouseConnector,
    "lever": LeverConnector,
    "direct_ats": DirectATSConnector,
    # Slowest board — last so a full-run still finishes the others first.
    "wearedevelopers": WeAreDevelopersConnector,
}

_SOURCE_CHOICES = tuple([*CONNECTORS.keys(), "all"])

# Job listing domains that block Playwright (bot detection / OAuth walls).
# URLs from these domains are opened in the user's default system browser.
SYSTEM_BROWSER_DOMAINS = {
    "remoteok.com",
    "weworkremotely.com",
    "jobicy.com",
    "getonbrd.com",
    "himalayas.app",
    "remotejobs.io",
    "remote.co/",
    "remotefront.com",
    "omnijobs.io",
}

_sqlite = str(config.DATABASE_URL).startswith("sqlite")
engine = create_engine(
    config.DATABASE_URL,
    connect_args={"timeout": 30, "check_same_thread": False} if _sqlite else {},
)
configure_sqlite(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
if engine.dialect.name == "sqlite":
    ensure_job_columns(engine)
    backfill_dedup_keys(engine)
    ensure_company_profiles(engine)

@click.group()
def cli():
    """Job Apply Agent Data Pipeline CLI."""
    pass

def _load_profile(profile_path: str):
    with open(profile_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def _should_preserve_final_status(job: Job) -> bool:
    if job.archived or job.status in ("applied", "expired", "deferred"):
        return True
    return bool(job.llm_status == "completed" and job.status not in (None, "new"))


def _apply_reject_reason(job: Job, scoring_result: dict, preserve_final_status: bool) -> None:
    code = scoring_result.get("reject_code")
    detail = scoring_result.get("reject_detail")
    if code:
        job.reject_code = code
        job.reject_detail = detail
        return
    if preserve_final_status and job.status == "rejected":
        return
    job.reject_code = None
    job.reject_detail = None


def _store_score(job: Job, scoring_result: dict) -> int:
    score = scoring_result.get("fit_score") or 0
    job.fit_score = score
    job.score_breakdown = dump_score_breakdown(scoring_result.get("score_breakdown"))
    return score


def _should_promote(job: Job, status: str, promote: bool, rule_score: int) -> bool:
    if not promote or status not in ("review", "new"):
        return False
    if rule_score >= SHORTLIST_MIN_SCORE:
        return True
    llm_score = job.llm_fit_score
    return llm_score is not None and llm_score >= SHORTLIST_MIN_SCORE


def _persist_raw_job(
    connector, raw_job, session, run, dry_run: bool, profile=None
) -> None:
    """Normalize one grabbed job and commit it immediately."""
    normalized = None
    try:
        normalized = connector.normalize(raw_job)
        logger.debug(
            f"Normalized job: '{normalized.get('title')}' at '{normalized.get('company')}'"
        )

        posted_date = normalized.get("posted_date")
        if posted_date:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
                days=max_job_age_days(connector.get_source_name())
            )
            if posted_date.tzinfo is None:
                posted_date = posted_date.replace(tzinfo=datetime.timezone.utc)
            if posted_date < cutoff:
                run.jobs_duplicates += 1
                if not dry_run:
                    session.commit()
                else:
                    session.rollback()
                return

        if is_duplicate(normalized, session):
            run.jobs_duplicates += 1
            if not dry_run:
                session.commit()
            else:
                session.rollback()
            return

        skip = exclusion_reason(normalized, profile)
        if skip:
            logger.debug(
                f"Skipped ingest ({skip[0]}): '{normalized.get('title')}' — {skip[1]}"
            )
            run.jobs_duplicates += 1
            if not dry_run:
                session.commit()
            else:
                session.rollback()
            return

        job_record = Job(**normalized)
        if not dry_run:
            session.add(job_record)
        run.jobs_new += 1
        if not dry_run:
            session.commit()
            mirror_job(job_record)
    except IntegrityError:
        session.rollback()
        run.jobs_duplicates += 1
        logger.debug(
            f"Skipped duplicate job (unique constraint): "
            f"{(normalized or {}).get('url')}"
        )
    except ScrapeHalted:
        session.rollback()
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing job: {e}")
        if not is_skippable_db_lock(e) and is_db_write_error(e):
            halt("db_write", str(e), source="sqlite")


def _drop_stored_ineligible(profile, dry_run: bool) -> None:
    if not profile:
        return
    session = SessionLocal()
    try:
        dropped = drop_ineligible_jobs(session, profile, dry_run=dry_run)
        if dropped:
            action = "Would drop" if dry_run else "Dropped"
            logger.info(
                f"{action} {dropped} stored jobs that fail remote/seniority/language/region rules"
            )
    except Exception as e:
        session.rollback()
        logger.warning(f"Failed to drop ineligible stored jobs: {e}")
    finally:
        session.close()


def _run_fetch(
    source: str,
    dry_run: bool,
    *,
    initial: bool = False,
    age_days: int | None = None,
    _skip_cleanup: bool = False,
):
    profile = load_candidate_profile()
    if not _skip_cleanup:
        _drop_stored_ineligible(profile, dry_run)
    if source == "all":
        selected = enabled_sources(list(CONNECTORS))
        skipped = [name for name in CONNECTORS if name not in set(selected)]
        if read_enabled() is None:
            logger.error(
                "full_run_sources.json is missing or invalid; no sources will be fetched. "
                "Choose connectors in the UI settings."
            )
        elif skipped:
            logger.info(
                f"Full-run sources: {len(selected)} selected; not selected: {', '.join(skipped)}"
            )
        else:
            logger.info(f"Full-run sources: {len(selected)} selected")
        for s in selected:
            try:
                _run_fetch(
                    s, dry_run, initial=initial, age_days=age_days, _skip_cleanup=True
                )
            except ScrapeHalted:
                logger.error(f"Stopped the remaining sources after {s}")
                return
        return

    if source not in CONNECTORS:
        logger.warning(f"Unknown source '{source}'. Available: {', '.join(CONNECTORS)}.")
        return

    days = resolve_job_age_days(source, initial=initial, age_days=age_days)
    logger.info(
        f"Starting fetch for source '{source}' (dry_run={dry_run}, age_days={days})"
    )
    with age_days_override(days):
        _run_fetch_source(source, dry_run, profile)


def _run_fetch_source(source: str, dry_run: bool, profile=None):
    session = SessionLocal()
    
    # Create PipelineRun record (only save if not dry-run)
    run = PipelineRun(
        source=source,
        started_at=datetime.datetime.now(datetime.timezone.utc),
        status="running",
        jobs_fetched=0,
        jobs_new=0,
        jobs_duplicates=0
    )
    
    if not dry_run:
        session.add(run)
        session.commit()
    
    try:
        connector_class = CONNECTORS[source]
        connector = connector_class()
        seen_raw_ids: set[int] = set()

        def persist(raw_job):
            raw_id = id(raw_job)
            if raw_id in seen_raw_ids:
                return
            seen_raw_ids.add(raw_id)
            run.jobs_fetched += 1
            _persist_raw_job(connector, raw_job, session, run, dry_run, profile)

        connector._on_raw_job = persist
        raw_jobs = connector.fetch_jobs()
        if raw_jobs:
            for raw_job in raw_jobs:
                persist(raw_job)

        run.status = "completed"
        run.completed_at = datetime.datetime.now(datetime.timezone.utc)
        if not dry_run:
            session.commit()
            
        msg = f"Pipeline completed: {run.jobs_fetched} fetched, {run.jobs_new} new, {run.jobs_duplicates} duplicates from {source}."
        if run.jobs_new:
            logger.warning(msg)
        else:
            logger.info(msg)
        alert("info", "report", msg, source=source)

    except ScrapeHalted as e:
        session.rollback()
        logger.error(f"Scraping stopped: {e}")
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.datetime.now(datetime.timezone.utc)
        if not dry_run:
            session.commit()
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Pipeline failed: {e}")
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.datetime.now(datetime.timezone.utc)
        if not dry_run:
            session.commit()
            
    finally:
        session.close()

def _run_evaluate(profile: str, dry_run: bool, all_jobs: bool):
    candidate_profile = _load_profile(profile)
    logger.info(f"Starting job evaluation using profile '{profile}' (dry_run={dry_run})")
    session = SessionLocal()
    
    try:
        dropped = drop_ineligible_jobs(session, candidate_profile, dry_run=dry_run)
        if dropped:
            action = "Would drop" if dry_run else "Dropped"
            logger.info(
                f"{action} {dropped} stored jobs that fail remote/seniority/language/region rules"
            )
        query = session.query(Job)
        if all_jobs:
            jobs_to_evaluate = query.filter(
                Job.status.notin_(["applied", "deferred", "expired"]),
                or_(Job.archived.is_(False), Job.archived.is_(None)),
            ).all()
        else:
            jobs_to_evaluate = query.filter(Job.status == "new").all()
        total_eval = len(jobs_to_evaluate)
        scope = "active queues" if all_jobs else "new"
        logger.info(f"Found {total_eval} {scope} jobs to evaluate.")
        
        counts = {"shortlisted": 0, "review": 0, "rejected": 0, "applied": 0, "errors": 0}
        final_counts = {"shortlisted": 0, "review": 0, "rejected": 0, "applied": 0, "new": 0}
        
        for idx, job in enumerate(jobs_to_evaluate, 1):
            try:
                # Utilities map off dictionaries; synthesize one from ORM
                job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
                
                # Check application history constraint natively
                if has_already_applied(job_dict, session):
                    job.rule_status = "applied"
                    job.status = "applied"
                    job.reject_code = "applied"
                    job.reject_detail = "Already in application history"
                    counts["applied"] += 1
                else:
                    scoring_result = score_job(job_dict, candidate_profile)
                    previous_status = job.status
                    preserve_final_status = _should_preserve_final_status(job)

                    _store_score(job, scoring_result)
                    job.remote_eligibility = scoring_result.get("remote_eligibility")
                    job.rule_status = scoring_result.get("recommended_status", "review")
                    _apply_reject_reason(job, scoring_result, preserve_final_status)

                    if previous_status in (None, "new"):
                        job.status = job.rule_status
                    elif previous_status == "applied":
                        job.status = "applied"
                    elif not preserve_final_status:
                        job.status = job.rule_status
                    
                    # Keep the resume recommendation in sync with the current active queue.
                    if job.status in ["shortlisted", "review"]:
                        resume_result = select_resume(job_dict, candidate_profile)
                        job.recommended_resume = resume_result.get("resume_name")
                    elif job.rule_status not in ["shortlisted", "review"] and not preserve_final_status:
                        job.recommended_resume = None
                        
                    # Count deterministic outcome separately from final status.
                    if job.rule_status in counts:
                        counts[job.rule_status] += 1
                    else:
                        counts["review"] += 1

                if job.status in final_counts:
                    final_counts[job.status] += 1
                else:
                    final_counts["review"] += 1

                if not dry_run and idx % 20 == 0:
                    session.commit()
                    
            except Exception as e:
                session.rollback()
                logger.error(f"Error evaluating job {job.id}: {e}")
                counts["errors"] += 1
                
        if not dry_run:
            session.commit()
            
        logger.info(
            "Evaluation complete. "
            f"Rule status -> Shortlisted: {counts['shortlisted']}, Review: {counts['review']}, "
            f"Rejected: {counts['rejected']}, Applied: {counts['applied']}, Errors: {counts['errors']}. "
            f"Final status -> Shortlisted: {final_counts['shortlisted']}, Review: {final_counts['review']}, "
            f"Rejected: {final_counts['rejected']}, Applied: {final_counts['applied']}."
        )
        
    except Exception as e:
        session.rollback()
        logger.error(f"Evaluation pipeline failed: {e}")
    finally:
        session.close()

def _run_analyze(profile: str, model: str, target_status: str, limit: int, dry_run: bool):
    candidate_profile = _load_profile(profile)
    limit = max(1, limit)
    logger.info(
        f"Starting LLM analysis using profile '{profile}', model '{model}', "
        f"status='{target_status}' (dry_run={dry_run}, limit={limit})"
    )
    session = SessionLocal()

    try:
        from sqlalchemy import or_
        jobs_to_analyze = (
            session.query(Job)
            .filter(
                Job.status == target_status,
                or_(Job.llm_status.is_(None), Job.llm_status == "failed"),
            )
            .order_by(Job.fit_score.desc(), Job.id.asc())
            .limit(limit)
            .all()
        )
        logger.info(f"Found {len(jobs_to_analyze)} jobs to analyze.")
        reachable, ollama_error = ollama_is_reachable()
        if not reachable:
            logger.error("%s", unreachable_hint(ollama_error))
            return

        counts = {"promoted": 0, "kept_review": 0, "rejected": 0, "failed": 0, "unchanged": 0}

        for idx, job in enumerate(jobs_to_analyze, 1):
            try:
                original_status = job.status
                job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
                analysis = analyze_job_with_ollama(job_dict, candidate_profile, model)

                if analysis.get("llm_status") == "failed":
                    job.llm_fit_score = None
                    job.llm_strengths = json.dumps([], ensure_ascii=False)
                    job.fit_explanation = None
                    job.skill_gaps = json.dumps([], ensure_ascii=False)
                    job.recommendation = None
                    job.llm_confidence = None
                    job.llm_status = "failed"
                    counts["failed"] += 1
                    logger.warning(
                        f"LLM analysis failed for '{job.title}' @ '{job.company}': "
                        f"{analysis.get('error', 'unknown error')}"
                    )
                else:
                    job.llm_fit_score = analysis.get("llm_fit_score")
                    job.llm_strengths = json.dumps(analysis.get("llm_strengths", []), ensure_ascii=False)
                    job.fit_explanation = analysis.get("fit_explanation")
                    job.skill_gaps = json.dumps(analysis.get("skill_gaps", []), ensure_ascii=False)
                    job.recommendation = analysis.get("recommendation")
                    job.llm_confidence = analysis.get("llm_confidence")
                    job.llm_status = analysis.get("llm_status")

                    if analysis.get("recommended_resume"):
                        job.recommended_resume = analysis.get("recommended_resume")

                    confidence = analysis.get("llm_confidence") or 0
                    recommendation = analysis.get("recommendation")

                    if recommendation == "shortlist" and confidence >= config.LLM_PROMOTION_CONFIDENCE:
                        job.status = "shortlisted"
                        job.reject_code = None
                        job.reject_detail = None
                        counts["promoted"] += 1
                    elif recommendation == "reject" and confidence >= config.LLM_PROMOTION_CONFIDENCE:
                        job.status = "rejected"
                        job.reject_code = "llm"
                        job.reject_detail = (analysis.get("fit_explanation") or "")[:400] or "LLM recommended reject"
                        counts["rejected"] += 1
                    elif original_status == "review":
                        job.status = "review"
                        counts["kept_review"] += 1
                    else:
                        job.status = original_status
                        counts["unchanged"] += 1

                    logger.info(
                        f"LLM analyzed '{job.title}' @ '{job.company}' -> "
                        f"recommendation={recommendation}, confidence={confidence}, status={job.status}"
                    )

                if not dry_run and idx % 10 == 0:
                    session.commit()
            except Exception as e:
                session.rollback()
                job.llm_status = "failed"
                counts["failed"] += 1
                logger.warning(f"Unexpected LLM analysis failure for job {job.id}: {e}")

        if not dry_run:
            session.commit()

        logger.info(
            "LLM analysis complete. "
            f"Analyzed {len(jobs_to_analyze)} {target_status} jobs: "
            f"promoted={counts['promoted']}, kept_review={counts['kept_review']}, "
            f"rejected={counts['rejected']}, unchanged={counts['unchanged']}, failed={counts['failed']}"
        )
    except Exception as e:
        session.rollback()
        logger.error(f"LLM analysis pipeline failed: {e}")
    finally:
        session.close()

def _display_jobs_by_status(target_status: str, limit: int):
    session = SessionLocal()
    try:
        jobs = (
            session.query(Job)
            .filter(Job.status == target_status)
            .order_by(Job.fit_score.desc(), Job.id.desc())
            .limit(max(1, limit))
            .all()
        )

        if not jobs:
            click.echo(f"No jobs found with status='{target_status}'.")
            return

        click.echo(f"{target_status.upper()} JOBS")
        click.echo("")

        for idx, job in enumerate(jobs, 1):
            fit_score = job.fit_score if job.fit_score is not None else "-"
            recommendation = job.recommendation or "-"
            confidence = job.llm_confidence if job.llm_confidence is not None else "-"
            resume = job.recommended_resume or "-"
            click.echo(f"{idx}. [id={job.id}] {job.title} -- {job.company}")
            click.echo(f"Score: {fit_score}")

            if recommendation != "-" and confidence != "-":
                click.echo(f"LLM: {recommendation} (confidence {confidence})")
            elif recommendation != "-":
                click.echo(f"LLM: {recommendation}")
            else:
                click.echo("LLM: -")

            click.echo(f"Resume: {resume}")

            if idx < len(jobs):
                click.echo("")
    finally:
        session.close()

@cli.command(name='triage')
def triage():
    """Work through review jobs one by one: shortlist, reject, skip, or open in browser."""
    import webbrowser

    session = SessionLocal()
    seen_ids: set = set()
    try:
        while True:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=10)
            job = (
                session.query(Job)
                .filter(
                    Job.status == 'review',
                    Job.id.notin_(seen_ids),
                    (Job.posted_date >= cutoff) | (Job.posted_date.is_(None)),
                )
                .order_by(Job.fit_score.desc(), Job.id.desc())
                .first()
            )
            if not job:
                click.echo("No more review jobs.")
                break

            click.echo("")
            click.echo(f"[id={job.id}] {job.title} -- {job.company}")
            click.echo(f"Score: {job.fit_score if job.fit_score is not None else '-'}  |  Source: {job.source or '-'}")
            if job.recommendation:
                click.echo(f"LLM:   {job.recommendation} (confidence {job.llm_confidence or '-'})")
            if job.fit_explanation:
                click.echo(f"Note:  {job.fit_explanation[:120]}")
            click.echo(f"URL:   {job.url or '-'}")
            click.echo("")
            click.echo("  [s] shortlist   [r] reject   [o] open in browser   [n] next   [q] quit")

            while True:
                choice = click.prompt("Action", default="o").strip().lower()
                if choice in ("s", "r", "o", "n", "q"):
                    break
                click.echo("  Please enter s, r, o, n, or q.")

            quit_flag = False
            if choice == "q":
                break
            elif choice == "s":
                job.status = "shortlisted"
                session.commit()
                click.echo(f"  → Shortlisted: {job.title}")
            elif choice == "r":
                job.status = "rejected"
                session.commit()
                click.echo(f"  → Rejected: {job.title}")
            elif choice == "o":
                if job.url:
                    webbrowser.open(job.url)
                    click.echo("  Opened in browser. Press ENTER to continue.")
                    try:
                        input()
                    except Exception:
                        pass
                    # Ask again after viewing
                    click.echo("  [s] shortlist   [r] reject   [a] applied   [n] next   [q] quit")
                    while True:
                        choice2 = click.prompt("Action", default="n").strip().lower()
                        if choice2 in ("s", "r", "a", "n", "q"):
                            break
                    if choice2 == "s":
                        job.status = "shortlisted"
                        session.commit()
                        click.echo(f"  → Shortlisted: {job.title}")
                    elif choice2 == "r":
                        job.status = "rejected"
                        session.commit()
                        click.echo(f"  → Rejected: {job.title}")
                    elif choice2 == "a":
                        job.status = "applied"
                        session.commit()
                        click.echo(f"  → Applied: {job.title}")
                    elif choice2 == "q":
                        quit_flag = True
                else:
                    click.echo("  No URL available.")

            seen_ids.add(job.id)
            session.expire_all()
            if quit_flag:
                break
    finally:
        session.close()

@cli.command()
@click.option('--source', required=True, type=click.Choice(_SOURCE_CHOICES), help='Job source to fetch from')
@click.option('--dry-run', is_flag=True, help='Run pipeline without inserting jobs into database')
@click.option('--initial', is_flag=True, help='Use the 30-day first-ingest window even if this source was fetched before')
@click.option('--age-days', type=int, default=None, help='Override job age window in days')
def fetch(source: str, dry_run: bool, initial: bool, age_days: int | None):
    """Fetch remote jobs from the specified source."""
    try:
        _run_fetch(source, dry_run, initial=initial, age_days=age_days)
    except ScrapeHalted as e:
        raise SystemExit(1) from e

@cli.command()
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--dry-run', is_flag=True, help='Evaluate without saving to DB')
@click.option('--all-jobs', is_flag=True, help='Re-evaluate jobs except applied, deferred, archived, and expired')
def evaluate(profile: str, dry_run: bool, all_jobs: bool):
    """Evaluate raw jobs against candidate profile and assign scores."""
    try:
        _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return
    _run_evaluate(profile, dry_run, all_jobs)

@cli.command()
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--model', default=config.OLLAMA_MODEL, help='Ollama model name')
@click.option('--status', 'target_status', default=config.LLM_STATUS_DEFAULT, type=click.Choice(['review', 'shortlisted', 'rejected']), help='Job status bucket to analyze')
@click.option('--limit', default=config.LLM_MAX_JOBS_PER_RUN, type=int, show_default=True, help='Maximum number of jobs to analyze')
@click.option('--dry-run', is_flag=True, help='Analyze without saving to DB')
def analyze(profile: str, model: str, target_status: str, limit: int, dry_run: bool):
    """Analyze selected jobs with Ollama and conservatively adjust status."""
    try:
        _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return
    _run_analyze(profile, model, target_status, limit, dry_run)

@cli.command(name='full-run')
@click.option('--source', default='all', type=click.Choice(_SOURCE_CHOICES), show_default=True, help='Job source to fetch from')
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--model', default=config.OLLAMA_MODEL, help='Ollama model name')
@click.option('--analyze-status', default=config.LLM_STATUS_DEFAULT, type=click.Choice(['review', 'shortlisted', 'rejected']), show_default=True, help='Job status bucket to analyze after evaluation')
@click.option('--analyze-limit', default=config.LLM_MAX_JOBS_PER_RUN, type=int, show_default=True, help='Maximum number of jobs to analyze')
@click.option('--dry-run', is_flag=True, help='Run the full pipeline without saving DB changes')
@click.option('--email', is_flag=True, help='Send email report if new shortlisted or review jobs were found')
@click.option('--initial', is_flag=True, help='Use the 30-day first-ingest window even if this source was fetched before')
@click.option('--age-days', type=int, default=None, help='Override job age window in days')
def full_run(source: str, profile: str, model: str, analyze_status: str, analyze_limit: int, dry_run: bool, email: bool, initial: bool, age_days: int | None):
    """Run fetch, evaluate, and analyze in one command."""
    from dotenv import load_dotenv
    load_dotenv()

    try:
        _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return

    logger.info(
        f"Starting full pipeline run: source='{source}', profile='{profile}', "
        f"model='{model}', analyze_status='{analyze_status}', analyze_limit={max(1, analyze_limit)}, "
        f"dry_run={dry_run}"
    )

    # Snapshot counts before run to detect new additions
    session = SessionLocal()
    try:
        from sqlalchemy import func
        before = dict(session.query(Job.status, func.count()).group_by(Job.status).all())
    finally:
        session.close()

    try:
        _run_fetch(source, dry_run, initial=initial, age_days=age_days)
    except ScrapeHalted as e:
        raise SystemExit(1) from e
    _run_evaluate(profile, dry_run, all_jobs=False)
    # Always analyze both buckets so no shortlisted/review job is left without
    # LLM metrics regardless of which bucket new jobs land in.
    for _status in ("review", "shortlisted"):
        if _status != analyze_status:
            _run_analyze(profile, model, _status, analyze_limit, dry_run)
    _run_analyze(profile, model, analyze_status, analyze_limit, dry_run)
    logger.info("Full pipeline run complete.")

    if not dry_run:
        click.echo("")
        for status in ("shortlisted", "review"):
            _display_jobs_by_status(status, limit=20)
        _print_stats()

    if email and not dry_run:
        session = SessionLocal()
        try:
            from sqlalchemy import func
            after = dict(session.query(Job.status, func.count()).group_by(Job.status).all())
            new_shortlisted = after.get("shortlisted", 0) - before.get("shortlisted", 0)
            new_review = after.get("review", 0) - before.get("review", 0)

            if new_shortlisted > 0 or new_review > 0:
                # Collect the newly promoted/added jobs for the email
                new_jobs = []
                if new_shortlisted > 0:
                    jobs = (session.query(Job)
                            .filter(Job.status == "shortlisted")
                            .order_by(Job.id.desc())
                            .limit(new_shortlisted).all())
                    new_jobs += [{"id": j.id, "title": j.title, "company": j.company,
                                  "fit_score": j.fit_score, "status": "shortlisted",
                                  "source": j.source, "url": j.url} for j in jobs]
                if new_review > 0:
                    jobs = (session.query(Job)
                            .filter(Job.status == "review")
                            .order_by(Job.id.desc())
                            .limit(new_review).all())
                    new_jobs += [{"id": j.id, "title": j.title, "company": j.company,
                                  "fit_score": j.fit_score, "status": "review",
                                  "source": j.source, "url": j.url} for j in jobs]
                send_report(new_jobs, after)
            else:
                logger.info("No new shortlisted or review jobs — skipping email report.")
        finally:
            session.close()

def _run_rescore(candidate_profile, status: str, promote: bool = False) -> str:
    """Re-score jobs in ``status``. Optionally promote high-scoring review/new rows."""
    session = SessionLocal()
    try:
        dropped = drop_ineligible_jobs(session, candidate_profile, dry_run=False)
        if dropped:
            logger.info(
                f"Dropped {dropped} stored jobs that fail remote/seniority/language/region rules"
            )
        jobs = session.query(Job).filter(Job.status == status).all()
        rejected = 0
        restored = 0
        promoted = 0
        for job in jobs:
            job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
            result = score_job(job_dict, candidate_profile)
            new_status = result["recommended_status"]
            score = _store_score(job, result)
            if new_status == "rejected":
                job.status = "rejected"
                job.rule_status = "rejected"
                job.reject_code = result.get("reject_code")
                job.reject_detail = result.get("reject_detail")
                if status != "rejected":
                    rejected += 1
            elif (
                status == "rejected"
                and (job.reject_code or "") == "remote"
            ):
                job.status = new_status
                job.rule_status = new_status
                job.reject_code = None
                job.reject_detail = None
                restored += 1
            elif _should_promote(job, status, promote, score):
                job.status = "shortlisted"
                job.rule_status = "shortlisted"
                job.reject_code = None
                job.reject_detail = None
                promoted += 1
            else:
                job.rule_status = new_status
        session.commit()
        kept = len(jobs) - rejected - restored - promoted
        msg = f"Rescored {len(jobs)} '{status}' jobs: {rejected} rejected"
        if promoted:
            msg += f", {promoted} promoted to shortlisted"
        if restored:
            msg += f", {restored} restored from location reject"
        msg += f", {kept} kept."
        return msg
    finally:
        session.close()


@cli.command()
@click.option('--profile', default='profile.yaml', show_default=True, help='Path to candidate profile YAML')
@click.option('--status', default='review', type=click.Choice(['review', 'new', 'shortlisted', 'rejected']), show_default=True, help='Job status bucket to rescore')
@click.option('--promote', is_flag=True, help=f'Move review/new jobs with fit_score or llm_fit_score >= {SHORTLIST_MIN_SCORE} to shortlisted')
def rescore(profile: str, status: str, promote: bool):
    """Re-run rule-based scoring on existing jobs and reject those that no longer qualify."""
    candidate_profile = _load_profile(profile)
    click.echo(_run_rescore(candidate_profile, status, promote=promote))

@cli.command(name='send-test-email')
def send_test_email():
    """Send a test email to verify credentials and SMTP settings."""
    from dotenv import load_dotenv
    load_dotenv()
    dummy_jobs = [
        {"id": 1, "title": "Test: Senior ML Engineer", "company": "Acme Corp", "fit_score": 75, "status": "shortlisted", "source": "remotive", "url": "https://remotive.com/jobs/example-ml-engineer"},
        {"id": 2, "title": "Test: Backend Engineer", "company": "Startup Inc", "fit_score": 52, "status": "review", "source": "arbeitnow", "url": "https://arbeitnow.com/jobs/example-backend-engineer"},
    ]
    dummy_counts = {"shortlisted": 3, "review": 12, "applied": 1, "deferred": 0, "rejected": 84}
    ok = send_report(dummy_jobs, dummy_counts)
    if ok:
        click.echo(click.style("Test email sent successfully.", fg="green"))
    else:
        click.echo(click.style("Failed to send test email — check logs above.", fg="red"))

@cli.command(name='setup-credentials')
def setup_credentials():
    """Store email credentials securely in Windows Credential Manager."""
    import keyring
    service = "job-apply-agent"
    click.echo("Storing email credentials in Windows Credential Manager (never written to disk).")
    click.echo("")
    email_from = click.prompt("Sender email address")
    email_to = click.prompt("Recipient email address", default=email_from)
    password = click.prompt("Gmail App Password", hide_input=True)
    keyring.set_password(service, "EMAIL_FROM", email_from)
    keyring.set_password(service, "EMAIL_TO", email_to)
    keyring.set_password(service, "EMAIL_PASSWORD", password)
    click.echo("")
    click.echo(click.style("Credentials saved to Windows Credential Manager.", fg="green"))
    click.echo("You can now remove EMAIL_FROM, EMAIL_TO, and EMAIL_PASSWORD from your .env file.")

@cli.command(name='help')
def help_command():
    """Show a summary of all available commands."""
    lines = [
        ("", "DAILY WORKFLOW", ""),
        ("full-run", "Fetch, evaluate and LLM-analyze new jobs", "--email  --source <src>  --dry-run"),
        ("triage", "Work through review jobs one by one (shortlist / reject / open)", ""),
        ("stats", "Show job counts by status + command tips", ""),
        ("", "", ""),
        ("", "BROWSING JOBS", ""),
        ("shortlist", "List shortlisted jobs", "--limit N"),
        ("review", "List review jobs", "--limit N"),
        ("rejected", "List rejected jobs", "--limit N"),
        ("deferred", "List deferred jobs", "--limit N"),
        ("open-job", "Open a job in browser and apply", "--status shortlisted|review  --job-id N"),
        ("", "", ""),
        ("", "PIPELINE TOOLS", ""),
        ("fetch", "Fetch only (no scoring/LLM). Use --source all for all sources", "--source <src>  --dry-run  --initial"),
        ("evaluate", "Score new jobs against your profile", "--profile  --dry-run"),
        ("analyze", "Run LLM analysis on review jobs", "--limit N  --model <model>  --dry-run"),
        ("rescore", "Re-apply scoring rules; optionally promote high scores", "--status review --promote"),
        ("refill-descriptions", "Hydrate stub RemoteJobsFinder descriptions from the detail API", "--dry-run"),
        ("dedup", "Drop extra copies of the same job already in SQLite", "--dry-run"),
        ("", "", ""),
        ("", "PERFORMANCE", ""),
        ("perf", "Plot prefill timing trend from recorded runs", "--job Coinbase  --last 5"),
        ("", "To record a run: $env:CC_PROFILE=\"1\"; python run_pipeline.py open-job --rank 2", ""),
        ("", "", ""),
        ("", "SETUP & EMAIL", ""),
        ("setup-credentials", "Store email credentials in Windows Credential Manager", ""),
        ("send-test-email", "Send a test email to verify SMTP setup", ""),
        ("ask", "Chat with the configured Ollama model for troubleshooting help", "--model <model>"),
        ("", "", ""),
        ("", "SOURCES", ""),
        ("", "remotive  remoteok  weworkremotely  arbeitnow  jobicy  jobspresso", ""),
        ("", "dynamitejobs  workingnomads  getonboard  himalayas  adzuna", ""),
        ("", "ashby  greenhouse  lever  direct_ats  nodesk  remote100k", ""),
        ("", "wearedistributed  flexa  remotejobsio  remotejobsfinder", ""),
        ("", "dailyremote  arcdev  flexjobs  ycombinator  waas  techjobsforgood  remotecom  remoteco  devremote  anywherepositions  remoterocketship  dice  workable  remotescout24  trulyremote  aijobs  aijobsai  justjoin  brenxor  jobgether  postjobfree  topsalaries  levelsfyi  workew  ladders  startupjobs  4dayweek  builtin  virtualvocations  up2staff  remotearmy  remoteyeah  remotesource  remotejobs  remotefrontjobs  remotefront  remotewlb  omnijobs  techcareers  smartrecruiters  hubstafftalent  tryremotely  findmyremote  jobdiva  wearedevelopers", ""),
        ("", "all = every registered source except flexjobs (paid board; --source flexjobs)", ""),
    ]

    HIGHLIGHT = {"full-run", "open-job"}

    click.echo("")
    click.echo(click.style("  Job Apply Agent — Command Reference", fg="cyan", bold=True))
    click.echo("")
    for cmd, desc, opts in lines:
        if not cmd and not desc and not opts:
            click.echo("")
        elif not cmd:
            click.echo(click.style(f"  {desc}", fg="yellow", bold=True))
        elif cmd in HIGHLIGHT:
            cmd_str = click.style(f"  {cmd:<22}", fg="magenta", bold=True)
            opts_str = click.style(f"  {opts}", fg="white") if opts else ""
            click.echo(f"{cmd_str}{desc}{opts_str}")
        else:
            cmd_str = click.style(f"  {cmd:<22}", fg="green")
            opts_str = click.style(f"  {opts}", fg="white") if opts else ""
            click.echo(f"{cmd_str}{desc}{opts_str}")
    click.echo("")

def _print_stats():
    session = SessionLocal()
    try:
        from sqlalchemy import func
        counts = dict(session.query(Job.status, func.count()).group_by(Job.status).all())
        order = ["shortlisted", "review", "deferred", "applied", "rejected", "new"]
        click.echo("")
        for status in order:
            n = counts.get(status, 0)
            if status in ("shortlisted", "review") and n > 0:
                click.echo(click.style(f"  {status:<12} {n}", fg="yellow", bold=True))
            else:
                click.echo(f"  {status:<12} {n}")
        extras = set(counts) - set(order)
        for status in sorted(extras):
            click.echo(f"  {status:<12} {counts[status]}")
        click.echo("")
        if counts.get("review", 0) > 0:
            click.echo(click.style("  Tip: run 'triage' to work through review jobs one by one.", fg="cyan"))
        click.echo(click.style("  Tip: run 'full-run' to fetch and evaluate new jobs.", fg="cyan"))
    finally:
        session.close()

@cli.command()
def stats():
    """Show a quick count of jobs by status."""
    _print_stats()


@cli.command()
@click.option('--days', default=14, type=int, show_default=True,
              help='Mark jobs older than this many days as expired')
@click.option('--dry-run', is_flag=True,
              help='Show what would be expired without writing to DB')
def prune(days: int, dry_run: bool):
    """Expire stale jobs that are still in review/shortlisted/new after --days days."""
    from datetime import datetime, timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    prunable_statuses = ("new", "review", "shortlisted")

    session = SessionLocal()
    try:
        jobs = (
            session.query(Job)
            .filter(Job.status.in_(prunable_statuses))
            .filter(Job.created_at < cutoff)
            .all()
        )

        if not jobs:
            click.echo(f"No stale jobs found (older than {days} days in {prunable_statuses}).")
            return

        by_status: dict = {}
        for job in jobs:
            by_status.setdefault(job.status, []).append(job)

        click.echo(f"{'[DRY RUN] ' if dry_run else ''}Expiring {len(jobs)} jobs older than {days} days:")
        for status, group in sorted(by_status.items()):
            click.echo(f"  {status}: {len(group)}")

        if not dry_run:
            for job in jobs:
                job.status = "expired"
            session.commit()
            click.echo(click.style(f"Done — {len(jobs)} jobs marked as expired.", fg="yellow"))
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@cli.command(name="refill-descriptions")
@click.option(
    "--source",
    default="remotejobsfinder",
    type=click.Choice(["remotejobsfinder"]),
    show_default=True,
    help="Board whose stub descriptions should be hydrated",
)
@click.option("--dry-run", is_flag=True, help="Count stubs without writing")
def refill_descriptions(source: str, dry_run: bool):
    """Fetch missing job-detail HTML for stored RemoteJobsFinder rows."""
    from connectors.remotejobsfinder import (
        hydrate_job_descriptions,
        needs_detail_description,
    )
    from utils.text_cleaning import clean_description

    session = SessionLocal()
    try:
        jobs = session.query(Job).filter(Job.source == source).all()
        stubs = [
            job
            for job in jobs
            if needs_detail_description(job.description or "")
            and job.external_id
            and not str(job.external_id).startswith("http")
        ]
        click.echo(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"{len(stubs)} of {len(jobs)} {source} jobs need a detail description."
        )
        if not stubs or dry_run:
            return
        payloads = [
            {"id": job.external_id, "description": job.description or ""}
            for job in stubs
        ]
        hydrate_job_descriptions(payloads)
        updated = 0
        failed = 0
        now = datetime.datetime.now(datetime.timezone.utc)
        by_id = {job.external_id: job for job in stubs}
        for payload in payloads:
            job = by_id.get(payload["id"])
            text = payload.get("description") or ""
            if job is None or needs_detail_description(text):
                failed += 1
                continue
            job.description = text
            job.description_text = clean_description(text)
            job.updated_at = now
            updated += 1
        session.commit()
        click.echo(f"Updated {updated} descriptions; {failed} still missing detail.")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@cli.command()
@click.option('--dry-run', is_flag=True, help='Show how many extras would be dropped without deleting')
def dedup(dry_run: bool):
    """Drop extra SQLite rows for the same posting on different URLs."""
    session = SessionLocal()
    try:
        groups, dropped = collapse_duplicate_jobs(session, dry_run=dry_run)
        prefix = "[DRY RUN] " if dry_run else ""
        if not dropped:
            click.echo(f"{prefix}No cross-URL duplicates found.")
            return
        click.echo(
            f"{prefix}Found {groups} duplicate groups; "
            f"{'would drop' if dry_run else 'dropped'} {dropped} extra jobs."
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@cli.command()
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of shortlisted jobs to display')
def shortlist(limit: int):
    """Display shortlisted jobs with rule and LLM summary fields."""
    _display_jobs_by_status("shortlisted", limit)

@cli.command(name='review')
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of review jobs to display')
def review_command(limit: int):
    """Display review jobs with rule and LLM summary fields."""
    _display_jobs_by_status("review", limit)

@cli.command()
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of rejected jobs to display')
def rejected(limit: int):
    """Display rejected jobs with rule and LLM summary fields."""
    _display_jobs_by_status("rejected", limit)

@cli.command()
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of deferred jobs to display')
def deferred(limit: int):
    """Display deferred jobs (unsupported ATS, pending better support)."""
    _display_jobs_by_status("deferred", limit)

@cli.command(name='open-job')
@click.option('--job-id', type=int, default=None, help='Specific job ID to open (any status)')
@click.option('--rank', type=int, default=None, help='1-based position in the ordered shortlist/queue (e.g. --rank 2 = 2nd job)')
@click.option('--status', 'queue_status', default='shortlisted', type=click.Choice(['shortlisted', 'review']), show_default=True, help='Queue to work through when no --job-id given')
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--headless', is_flag=True, default=False, help='Run headless (default: headful)')
@click.option('--fill/--no-fill', default=True, help='Auto-fill detected form fields from profile (default: on)')
@click.option('--dry-run', is_flag=True, default=False, help='Show what would be filled without touching the form')
def open_job(job_id, rank, queue_status, profile, headless, fill, dry_run):
    """Open a shortlisted (or review) job in the browser, inspect the form, and record the outcome."""
    from playwright.async_api import async_playwright
    from utils.form_inspector import try_click_apply, scan_fields, format_field_report, extract_apply_url
    from utils.form_filler import fill_form, format_fill_report, try_upload_resume
    from utils.ats_detector import detect_ats, MANUAL_ONLY_ATS

    try:
        candidate_profile = _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return

    session = SessionLocal()
    seen_ids: set[int] = set()

    # Resolve --rank to a concrete job_id before entering the loop.
    if rank is not None:
        ranked_jobs = (
            session.query(Job)
            .filter(Job.status == queue_status, Job.url.isnot(None), Job.url != '')
            .order_by(Job.fit_score.desc(), Job.id.desc())
            .all()
        )
        if rank < 1 or rank > len(ranked_jobs):
            click.echo(f"Rank {rank} is out of range — only {len(ranked_jobs)} {queue_status} job(s) available.")
            session.close()
            return
        job_id = ranked_jobs[rank - 1].id

    try:
        while True:
            if job_id is not None:
                job = session.query(Job).filter(Job.id == job_id).first()
                if not job:
                    click.echo(f"No job with id={job_id} found.")
                    return
            else:
                job = (
                    session.query(Job)
                    .filter(
                        Job.status == queue_status,
                        Job.url.isnot(None),
                        Job.url != '',
                        Job.id.notin_(seen_ids),
                    )
                    .order_by(Job.fit_score.desc(), Job.id.desc())
                    .first()
                )
                if not job:
                    click.echo(f"No more {queue_status} jobs. All done!")
                    return

            click.echo("")
            click.echo(f"JOB:     {job.title}")
            click.echo(f"Company: {job.company}")
            click.echo(f"Score:   {job.fit_score if job.fit_score is not None else '-'}")
            if job.recommendation:
                click.echo(f"LLM:     {job.recommendation} (confidence {job.llm_confidence or '-'})")
            click.echo(f"Resume:  {job.recommended_resume or '-'}")
            click.echo(f"ATS:     {job.ats_type or 'unknown'}")
            click.echo(f"URL:     {job.url}")
            click.echo("")

            if not click.confirm("Open this job?", default=True):
                seen_ids.add(int(job.id))
                session.expire_all()
                continue

            job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
            target_url = job.url
            session_result = {"outcome": "done"}  # mutable sentinel shared with async closure

            # Some listing sites block Playwright (bot detection / OAuth walls).
            # Open these in the user's default browser where their session is live.
            _url_lower = (target_url or "").lower()
            if any(domain in _url_lower for domain in SYSTEM_BROWSER_DOMAINS):
                import webbrowser
                source_label = job.source or "listing site"
                click.echo(f"{source_label} — opening in your default browser (bot protection blocks automated browser).")
                webbrowser.open(target_url)
                click.echo("Press ENTER when you are done applying.")
                try:
                    input()
                except Exception:
                    pass
                # Fall through to "Did you apply?" below.
                seen_ids.add(job.id)
                click.echo("")
                if click.confirm("Did you apply?", default=False):
                    job.status = "applied"
                    existing = (
                        session.query(ApplicationHistory)
                        .filter_by(company=job.company, job_title=job.title)
                        .first()
                    )
                    if not existing:
                        history = ApplicationHistory(
                            company=job.company,
                            job_title=job.title,
                            applied_date=datetime.date.today(),
                            source=job.source or "manual",
                        )
                        session.add(history)
                    session.commit()
                    click.echo(f"Marked as applied: {job.title} @ {job.company}")
                else:
                    click.echo("No changes recorded.")
                if job_id is not None:
                    return
                continue

            async def _browser_session():
                async with async_playwright() as pw:
                    browser = await pw.chromium.launch(headless=headless)
                    page = await browser.new_page()

                    try:
                        await page.goto(target_url, wait_until="load", timeout=30000)
                    except Exception as e:
                        click.echo(f"Page load error: {e}")
                        await browser.close()
                        return

                    # On listing aggregators (Remotive etc.) try to resolve the
                    # direct employer apply URL before doing anything else.
                    resolved = await extract_apply_url(page)
                    if resolved and resolved != target_url:
                        click.echo(f"Resolved apply URL: {resolved}")
                        try:
                            await page.goto(resolved, wait_until="load", timeout=30000)
                            await page.wait_for_timeout(2000)
                        except Exception as e:
                            click.echo(f"Could not navigate to resolved URL: {e}")

                    active_page = page
                    clicked, active_page = await try_click_apply(active_page)
                    if clicked:
                        click.echo(f"Apply button clicked. Now at: {active_page.url}")
                        await active_page.wait_for_timeout(2000)
                    else:
                        click.echo("No Apply button detected — scanning current page.")

                    ats = detect_ats(active_page.url)
                    if ats in MANUAL_ONLY_ATS:
                        click.echo(f"\nATS '{ats}' — auto-fill not supported for this platform.")
                        if not click.confirm("Fill manually in the browser?", default=True):
                            click.echo("Skipping — browser will close.")
                            session_result["outcome"] = "skipped"
                            await browser.close()
                            return

                    try:
                        fields = await scan_fields(active_page)
                    except Exception as scan_err:
                        click.echo(f"Page closed unexpectedly during scan ({scan_err}). Navigate manually.")
                        fields = []
                    if fields:
                        click.echo(f"\nForm fields detected ({len(fields)}, * = required):")
                        click.echo(format_field_report(fields))
                    else:
                        click.echo("\nNo form fields detected yet (may need manual navigation).")

                    if fill and fields and ats not in MANUAL_ONLY_ATS:
                        mode = "DRY RUN — " if dry_run else ""
                        click.echo(f"\n{mode}Filling form from profile...")
                        _timing = os.environ.get("CC_PROFILE") == "1"
                        _collector = _TimingCollector(click.echo) if _timing else False
                        actions = await fill_form(active_page, fields, candidate_profile, job_dict, dry_run=dry_run, timing=_collector)
                        click.echo(format_fill_report(actions))
                        filled = sum(1 for a in actions if a["action"] in ("filled", "checked", "selected"))
                        skipped = sum(1 for a in actions if a["action"] == "skipped")
                        errors = sum(1 for a in actions if a["action"] == "error")
                        click.echo(f"\n  filled={filled}  skipped={skipped}  errors={errors}")
                        if _timing and isinstance(_collector, _TimingCollector):
                            summary = _collector.summary()
                            if summary:
                                click.echo(summary)
                            job_label = f"{job_dict.get('title', '')} @ {job_dict.get('company', '')}".strip(" @")
                            _collector.save(job_label)

                    if fill and ats not in MANUAL_ONLY_ATS:
                        resume_status = await try_upload_resume(active_page, candidate_profile, job_dict, dry_run=dry_run)
                        click.echo(f"  resume: {resume_status}")

                    click.echo("")
                    click.echo("Browser is open. Press ENTER here when you are done.")
                    try:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, input)
                    except Exception:
                        pass

                    try:
                        await browser.close()
                    except Exception:
                        pass

            try:
                asyncio.run(_browser_session())
            except Exception as e:
                click.echo(f"Browser session ended: {e}")

            if session_result["outcome"] == "skipped":
                seen_ids.add(job.id)
                session.expire_all()
                continue

            click.echo("")
            if click.confirm("Did you apply?", default=False):
                job.status = "applied"
                existing = (
                    session.query(ApplicationHistory)
                    .filter_by(company=job.company, job_title=job.title)
                    .first()
                )
                if not existing:
                    history = ApplicationHistory(
                        company=job.company,
                        job_title=job.title,
                        applied_date=datetime.date.today(),
                        source=job.source or "manual",
                    )
                    session.add(history)
                session.commit()
                click.echo(f"Marked as applied: {job.title} @ {job.company}")
            else:
                click.echo("No changes recorded.")

            # After each job (applied or not), stop if a specific id was requested.
            if job_id is not None:
                return

            seen_ids.add(job.id)
            session.expire_all()

    finally:
        session.close()


_SYSTEM_PROMPT = """You are an AI assistant embedded inside Job Apply Agent, a local job discovery and application pipeline.
You have access to tools that let you query the live jobs database and pipeline history.
Always use the appropriate tool to answer questions about jobs, counts, or pipeline runs — never guess or make up data.

== PROJECT OVERVIEW ==
Job Apply Agent fetches remote job listings from multiple sources (Remotive, RemoteOK, Adzuna, WeWorkRemotely, Himalayas, Greenhouse, Ashby, Lever, Workable, and others), scores them against a candidate profile, runs LLM analysis via Ollama (local or cloud, from OLLAMA_MODE in config.py), and assists with application form prefilling using Playwright.

== KEY FILES ==
- run_pipeline.py       — CLI entrypoint (commands: full-run, fetch, evaluate, analyze, triage, open-job, stats, ask)
- profile.yaml          — Candidate config: skills, target_roles, seniority, accepted_regions, target_companies, blacklisted_companies, resumes
- profile.template.yaml — Template to copy when setting up for the first time
- config.py             — Settings (DATABASE_URL, OLLAMA_MODE, OLLAMA_URL, OLLAMA_MODEL, email, etc.)
- .env                  — Secrets (OLLAMA_API_KEY, Adzuna API keys, email credentials); never committed to git
- connectors/           — One file per job source; all implement BaseConnector (fetch_jobs + normalize)
- utils/remote_filter.py — classify_remote_eligibility(): accept / review / reject
- utils/form_filler.py  — Playwright form prefill logic
- utils/form_inspector.py — Scans ATS form fields (supports aria-label, aria-labelledby, label[for=...])
- utils/llm_analysis.py — Sends jobs to Ollama for semantic scoring
- utils/logger.py       — Colored console + rotating file logger (logs/job_apply_agent.log)
- models/database.py    — SQLAlchemy models: Job, PipelineRun, ApplicationHistory

== COMMON ERRORS AND FIXES ==
- "No module named X"            → Run: pip install -r requirements.txt (in the job-apply-agent conda env)
- "Ollama connection refused"    → Local: ollama serve. Cloud: OLLAMA_MODE = "cloud" in config.py and OLLAMA_API_KEY in .env
- "model not found"              → Local: ollama pull qwen3.5:4b. Cloud: set OLLAMA_CLOUD_MODEL in config.py
- "profile.yaml not found"      → Copy profile.template.yaml to profile.yaml and fill in your details
- "DATABASE_URL not set"         → Check .env file exists and has the right values; copy .env.example if missing
- "NoneType has no attribute"    → Usually a missing field in profile.yaml or a job with null data — check logs
- "403 Forbidden" from RemoteOK  → Normal; that source requires a subscription for some jobs — pipeline skips them
- "404 from Ashby/Greenhouse"    → Company slug is wrong or the company moved ATS — update careers_url in profile.yaml
- Playwright browser doesn't open → Run: playwright install chromium
- LinkedIn field not filling      → Fixed in form_inspector.py via aria-labelledby; ensure you have the latest code
- Job marked shortlisted but wrong → Run: python run_pipeline.py rescore to re-apply rules

== PIPELINE COMMANDS ==
python run_pipeline.py full-run [--email] [--source all]   # Full fetch + score + LLM in one shot
python run_pipeline.py fetch --source all [--initial] [--age-days N]  # Fetch only
python run_pipeline.py evaluate                            # Score fetched jobs
python run_pipeline.py analyze                             # LLM pass on review jobs
python run_pipeline.py rescore [--promote]                 # Re-score review jobs; --promote shortlists rule or LLM 60+
python run_pipeline.py triage                              # Work through review queue
python run_pipeline.py open-job                            # Open & prefill application form
python run_pipeline.py stats                               # Job counts by status
python run_pipeline.py ask                                 # This assistant

== CONFIGURATION TIPS ==
- OLLAMA_MODE in config.py (local or cloud) — cloud uses https://ollama.com with OLLAMA_API_KEY in .env
- OLLAMA_LOCAL_MODEL / OLLAMA_CLOUD_MODEL in config.py — local qwen2.5:3b; cloud gpt-oss:20b
- To add a target company: add a line under target_companies in profile.yaml with name + careers_url
- To blacklist a company: add its name under blacklisted_companies in profile.yaml
- accepted_regions controls remote eligibility — add regions you can work from (emea, europe, canada, worldwide…)
- resumes: each resume has tags — the pipeline picks the best match per job

== AUTOMATED SCHEDULE ==
Use the get_schedule tool to answer any questions about when Job Apply Agent runs automatically.
To run manually at any time: python run_pipeline.py full-run

Answer questions clearly and concisely. If you are unsure, say so rather than guessing.
"""


def _read_recent_logs(n_lines: int = 60) -> str:
    log_path = os.path.join(os.path.dirname(__file__), "logs", "job_apply_agent.log")
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail = "".join(lines[-n_lines:]).strip()
        return tail if tail else "(no log entries yet)"
    except FileNotFoundError:
        return "(log file not found)"


@cli.command()
@click.option('--model', default=config.OLLAMA_MODEL, show_default=True, help='Ollama model to use')
def ask(model: str):
    """Start an interactive assistant chat powered by the configured Ollama model (with live DB access)."""
    import requests as _requests
    from utils.ask_tools import TOOL_SCHEMAS, dispatch_tool, ACTION_TOOLS, tool_policy_check, confirmation_prompt

    reachable, ollama_error = ollama_is_reachable()
    if not reachable:
        click.echo(click.style(unreachable_hint(ollama_error), fg="red"))
        return

    session = SessionLocal()
    recent_logs = _read_recent_logs()
    log_context = f"\n\n== RECENT LOG OUTPUT (last 60 lines) ==\n{recent_logs}"
    messages = [{"role": "system", "content": _SYSTEM_PROMPT + log_context}]

    click.echo("")
    click.echo(click.style("  Job Apply Agent Assistant", fg="cyan", bold=True))
    click.echo(click.style(f"  Model: {model}  |  DB access: enabled  |  Type 'exit' to quit", fg="white"))
    click.echo("")

    try:
        while True:
            try:
                click.echo(click.style("You: ", fg="green", bold=True), nl=False)
                user_input = input()
            except (EOFError, KeyboardInterrupt):
                break

            if user_input.strip().lower() in ("exit", "quit", "q"):
                break
            if not user_input.strip():
                continue

            messages.append({"role": "user", "content": user_input})

            # ---- inner tool-calling loop ----------------------------------------
            # Keep calling the LLM until it stops requesting tools and gives a
            # final text response.
            while True:
                payload = {
                    "model": model,
                    "messages": messages,
                    "tools": TOOL_SCHEMAS,
                    "stream": False,
                }

                click.echo(click.style("Assistant: ", fg="cyan", bold=True), nl=False)
                click.echo(click.style("(thinking...)", fg="yellow"), nl=False)

                try:
                    resp = _requests.post(
                        config.OLLAMA_URL,
                        json=payload,
                        headers=request_headers(),
                        timeout=120,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    click.echo(click.style(f"\nError contacting Ollama: {e}", fg="red"))
                    messages.pop()  # discard unanswered user message
                    break

                msg = data.get("message", {})
                # Ollama requires the full assistant message in history before tool results
                messages.append(msg)

                tool_calls = msg.get("tool_calls") or []

                if not tool_calls:
                    # Final response — print it
                    click.echo("\r" + " " * 50 + "\r", nl=False)
                    click.echo(click.style("Assistant: ", fg="cyan", bold=True), nl=False)
                    click.echo(msg.get("content", "").strip())
                    click.echo("")
                    break

                # ---- execute each requested tool --------------------------------
                click.echo("\r" + " " * 50 + "\r", nl=False)
                for tc in tool_calls:
                    fn_name = tc.get("function", {}).get("name", "")
                    fn_args = tc.get("function", {}).get("arguments") or {}
                    if isinstance(fn_args, str):
                        try:
                            fn_args = json.loads(fn_args)
                        except json.JSONDecodeError:
                            fn_args = {}

                    click.echo(click.style(f"  → {fn_name}({fn_args})", fg="yellow"))

                    if fn_name in ACTION_TOOLS:
                        # Policy gate — check preconditions before prompting
                        policy = tool_policy_check(fn_name, fn_args, session)
                        if "error" in policy:
                            result = policy
                        else:
                            prompt = confirmation_prompt(fn_name, fn_args, session)
                            click.echo(click.style(f"  {prompt} [y/N] ", fg="yellow"), nl=False)
                            try:
                                answer = input().strip().lower()
                            except (EOFError, KeyboardInterrupt):
                                answer = "n"
                            if answer == "y":
                                result = dispatch_tool(fn_name, fn_args, session)
                            else:
                                result = {"cancelled": True, "message": "Action cancelled by user."}
                    else:
                        result = dispatch_tool(fn_name, fn_args, session)

                    messages.append({
                        "role": "tool",
                        "content": json.dumps(result, default=str, ensure_ascii=False),
                    })
                # loop back — LLM sees tool results and generates final response
    finally:
        session.close()


@cli.command(name='cover-letter')
@click.option('--job-id', required=True, type=int, help='ID of the shortlisted job')
@click.option('--profile', default='profile.yaml', show_default=True, help='Path to candidate profile YAML')
@click.option('--model', default=config.OLLAMA_MODEL, show_default=True, help='Ollama model name')
@click.option('--regenerate', is_flag=True, help='Overwrite existing cover letter')
def cover_letter_cmd(job_id: int, profile: str, model: str, regenerate: bool):
    """Generate a tailored cover letter for a shortlisted job and save it to the DB."""
    from utils.cover_letter import generate_cover_letter

    profile_data = _load_profile(profile)
    if not profile_data:
        click.echo(click.style(f"Could not load profile from {profile}", fg="red"))
        return

    session = SessionLocal()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            click.echo(click.style(f"Job {job_id} not found.", fg="red"))
            return
        if job.status != "shortlisted":
            click.echo(click.style(
                f"Job {job_id} has status='{job.status}'. Cover letters are only generated for shortlisted jobs.",
                fg="yellow",
            ))
            return
        if job.cover_letter and not regenerate:
            click.echo(click.style("Cover letter already exists (use --regenerate to overwrite):\n", fg="cyan"))
            click.echo(job.cover_letter)
            return

        click.echo(f"Generating cover letter for: {job.title} @ {job.company} (model: {model})...")

        job_dict = {
            "title": job.title,
            "company": job.company,
            "location": job.location,
            "raw_location_text": job.raw_location_text,
            "description": job.description,
            "description_text": job.description_text,
            "fit_explanation": job.fit_explanation,
            "llm_strengths": job.llm_strengths,
        }

        result = generate_cover_letter(job_dict, profile_data, model=model)

        if result["status"] == "ok":
            job.cover_letter = result["cover_letter"]
            session.commit()
            click.echo(click.style("\n--- Cover Letter ---\n", fg="cyan"))
            click.echo(result["cover_letter"])
            click.echo(click.style("\n--- End ---", fg="cyan"))
        else:
            click.echo(click.style(f"Generation failed: {result.get('error')}", fg="red"))
    finally:
        session.close()


@cli.command(name='interview-prep')
@click.argument('job_application_id', type=int)
@click.option('--profile', default='profile.yaml', show_default=True, help='Path to candidate profile YAML')
def interview_prep_cmd(job_application_id: int, profile: str):
    """Generate a personalized interview prep sheet for a job application."""
    from utils.interview_prep import run_interview_prep

    profile_data = _load_profile(profile)
    if not profile_data:
        click.echo(click.style(f"Could not load profile from {profile}", fg="red"), err=True)
        sys.exit(1)

    def _progress(step: int, msg: str) -> None:
        click.echo(f"[{step}/6] {msg}")

    session = SessionLocal()
    try:
        sheet = run_interview_prep(job_application_id, profile_data, session, progress_callback=_progress)
        click.echo(click.style(f"\n✅ Interview prep sheet saved (ID: {sheet.id})", fg="green"))

        import json as _json
        sections = [
            ("Company Snapshot", sheet.company_snapshot),
            ("Role Summary", sheet.role_requirements_summary),
            ("Technical Questions", sheet.likely_technical_questions),
            ("Behavioral Questions", sheet.likely_behavioral_questions),
            ("Talking Points", sheet.talking_points),
            ("Gaps / Risks", sheet.gaps_or_risks),
            ("30-Min Prep Plan", sheet.prep_plan_30_min),
        ]
        for title, raw in sections:
            click.echo(click.style(f"\n--- {title} ---", fg="cyan"))
            try:
                click.echo(_json.dumps(_json.loads(raw), indent=2, ensure_ascii=False))
            except (TypeError, ValueError):
                click.echo(str(raw))
    except (ValueError, RuntimeError) as exc:
        click.echo(click.style(str(exc), fg="red"), err=True)
        sys.exit(1)
    finally:
        session.close()


@cli.command(name='perf')
@click.option('--job', default=None, help='Filter to runs matching this job substring (e.g. "Coinbase")')
@click.option('--last', default=10, type=int, show_default=True, help='Number of most-recent runs to plot')
@click.option('--save-only', is_flag=True, help='Save PNG without opening it')
def perf_cmd(job, last, save_only):
    """Plot prefill timing trends from logs/timing.jsonl."""
    import webbrowser
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker

    log_path = os.path.join(os.path.dirname(__file__), "logs", "timing.jsonl")
    if not os.path.exists(log_path):
        click.echo("No timing data yet — run with CC_PROFILE=1 first.")
        return

    records = []
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    if job:
        records = [r for r in records if job.lower() in r.get("job", "").lower()]

    if not records:
        click.echo(f"No records found{f' matching {job!r}' if job else ''}.")
        return

    records = records[-last:]
    click.echo(f"Plotting {len(records)} run(s){f' for jobs matching {job!r}' if job else ''}.")

    # Collect all op categories across all records
    all_cats = sorted({cat for r in records for cat in r.get("ops", {})})
    colors = plt.cm.tab10.colors  # type: ignore[attr-defined]
    cat_color = {cat: colors[i % len(colors)] for i, cat in enumerate(all_cats)}

    labels = []
    for r in records:
        ts = r.get("ts", "")[:16].replace("T", "\n")
        j = r.get("job", "")
        short_job = j.split("@")[0].strip()[:20] if j else ""
        labels.append(f"{ts}\n{short_job}" if short_job else ts)

    fig, ax = plt.subplots(figsize=(max(8, len(records) * 1.4), 5))

    bottoms = [0.0] * len(records)
    for cat in all_cats:
        vals = [r.get("ops", {}).get(cat, {}).get("total_ms", 0) / 1000 for r in records]
        ax.bar(range(len(records)), vals, bottom=bottoms, label=cat, color=cat_color[cat], width=0.6)
        bottoms = [b + v for b, v in zip(bottoms, vals)]

    # Total label above each bar
    for i, r in enumerate(records):
        total_s = r.get("total_ms", 0) / 1000
        ax.text(i, bottoms[i] + 0.05, f"{total_s:.1f}s", ha="center", va="bottom", fontsize=8, fontweight="bold")

    ax.set_xticks(range(len(records)))
    ax.set_xticklabels(labels, fontsize=7)
    ax.set_ylabel("Seconds")
    ax.set_title(f"Prefill timing — last {len(records)} run(s){f' · {job}' if job else ''}")
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1fs"))
    ax.legend(loc="upper right", fontsize=8)
    ax.set_ylim(0, max(bottoms) * 1.18)
    fig.tight_layout()

    out_path = os.path.join(os.path.dirname(__file__), "logs", "timing_trend.png")
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    click.echo(f"Saved: {out_path}")
    if not save_only:
        webbrowser.open(out_path)


@cli.command(name='ui')
@click.option('--port', default=7860, type=int, show_default=True)
@click.option('--no-browser', is_flag=True, help='Do not open browser automatically')
def ui_cmd(port: int, no_browser: bool):
    """Launch the Job Apply Agent web UI."""
    import socket
    import webbrowser
    import uvicorn

    url = f"http://localhost:{port}"

    # Kill any process already holding the port so we always run the latest code.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
        if _s.connect_ex(("127.0.0.1", port)) == 0:
            import psutil
            killed = False
            for proc in psutil.process_iter(["pid"]):
                try:
                    for conn in proc.connections(kind="inet"):
                        if conn.laddr.port == port:
                            click.echo(f"Killing existing UI process (PID {proc.pid}) on port {port}.")
                            proc.kill()
                            proc.wait(timeout=5)
                            killed = True
                            break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            if not killed:
                click.echo(f"Port {port} in use but could not identify process — proceeding anyway.")

    if not no_browser:
        webbrowser.open(url)
    click.echo(f"Starting Job Apply Agent UI at {url}")
    uvicorn.run("ui.app:app", host="127.0.0.1", port=port, reload=False, timeout_graceful_shutdown=3)


if __name__ == '__main__':
    cli()
