"""Tell the administrator what a scrape did, and when it needs a person.

Grades that are delivered:
- ``info`` — the scrape finished. This is the completion report.
- ``action`` — the administrator must do something. A sign-in they can
  finish in the open browser pauses and waits. ``halt`` stops the scrape
  when this session cannot recover, such as a profile that will not open
  or a database that rejected the write.

Skipped pages, database locks, and other warnings stay in the log.
They are not sent.

Each incident is sent once per process. ``halt`` sends an action alert
and raises ``ScrapeHalted`` so the fetch stops. A scraper that can wait
for the administrator calls ``alert`` and keeps the session open.
Add a channel by appending a ``(name, factory)`` pair to ``_CHANNELS``.
The factory returns a ``send(text)`` function, or None when that channel
is not configured.

Set ``ALERT_CHANNELS`` to a comma-separated list, such as
``email,slack``. A listed channel is skipped when its credentials
are empty. When ``ALERT_CHANNELS`` is unset, incidents are only logged.
"""
from __future__ import annotations

import json
import os
import re
import smtplib
from email.mime.text import MIMEText
from urllib.request import Request, urlopen

from utils.logger import setup_logger

logger = setup_logger("alerts")

_sent: set[tuple[str, str, str]] = set()
_SECRET_URL = re.compile(r"://[^/\s:]+:[^/\s@]+@")
_DB_ERROR_NAMES = {"OperationalError", "DatabaseError"}
_LABELS = {
    "info": "INFO",
    "warning": "WARNING",
    "action": "NEED ACTION",
}


class ScrapeHalted(RuntimeError):
    """An action-grade incident stopped the scrape."""


def alert(level: str, kind: str, message: str, *, source: str = "") -> None:
    """Send one graded incident. Later calls with the same grade, kind, and source are skipped."""
    if level not in _LABELS:
        raise ValueError(f"unknown alert grade {level!r}")
    if level == "warning":
        logger.debug(f"not alerted ({kind}): {message}")
        return
    key = (level, kind, source or "")
    if key in _sent:
        return
    text = _public_text(level, kind, message, source)
    channels = _open_channels()
    if not channels:
        _sent.add(key)
        log = logger.info if level == "info" else logger.warning
        log(f"alert not delivered (no channel configured): {text}")
        return
    delivered = False
    for name, send in channels:
        try:
            send(text)
        except Exception as e:
            logger.warning(f"alert channel {name} failed: {e}")
            continue
        delivered = True
        logger.info(
            f"alert sent via {name}: {_LABELS[level]} {kind}"
            + (f" ({source})" if source else "")
        )
    if delivered:
        _sent.add(key)


def halt(kind: str, message: str, *, source: str = "") -> None:
    """Alert at need-action grade and stop the scrape."""
    alert("action", kind, message, source=source)
    raise ScrapeHalted(message)


def is_skippable_db_lock(exc: BaseException) -> bool:
    """A lock the fetch can skip while the rest of the scrape continues."""
    text = str(exc).lower()
    return "database is locked" in text or "database is busy" in text


def is_db_write_error(exc: BaseException) -> bool:
    if is_skippable_db_lock(exc):
        return True
    return type(exc).__name__ in _DB_ERROR_NAMES


def _public_text(level: str, kind: str, message: str, source: str) -> str:
    where = f" ({source})" if source else ""
    body = _SECRET_URL.sub("://", message or "").strip()
    if len(body) > 3500:
        body = body[:3500] + "…"
    headline = (
        "Scraping report"
        if level == "info"
        else "Job Apply Agent needs attention"
    )
    return f"{_LABELS[level]}\n{headline}\n{kind}{where}\n{body}"


def _open_channels() -> list[tuple[str, object]]:
    selected = {
        name.strip().lower()
        for name in (os.getenv("ALERT_CHANNELS") or "").split(",")
        if name.strip()
    }
    if not selected:
        return []
    opened = []
    for name, factory in _CHANNELS:
        if name not in selected:
            continue
        send = factory()
        if send is not None:
            opened.append((name, send))
    return opened


def _email_channel():
    from utils.email_report import _get_credential

    email_from = _get_credential("EMAIL_FROM")
    email_to = (os.getenv("ALERT_EMAIL_TO") or "").strip() or _get_credential("EMAIL_TO")
    email_password = _get_credential("EMAIL_PASSWORD")
    if not all([email_from, email_to, email_password]):
        return None
    host = _get_credential("EMAIL_SMTP_HOST") or "smtp.gmail.com"
    port = int(_get_credential("EMAIL_SMTP_PORT") or "587")

    def send(text: str) -> None:
        msg = MIMEText(text)
        grade = text.split("\n", 1)[0]
        msg["Subject"] = f"Job Apply Agent {grade}"
        msg["From"] = email_from
        msg["To"] = email_to
        with smtplib.SMTP(host, port, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(email_from, email_password)
            server.sendmail(email_from, email_to, msg.as_string())

    return send


def _slack_channel():
    url = (os.getenv("SLACK_WEBHOOK_URL") or "").strip()
    if not url:
        return None

    def send(text: str) -> None:
        _post_json(url, {"text": text})

    return send


def _telegram_channel():
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        return None

    def send(text: str) -> None:
        _post_json(
            f"https://api.telegram.org/bot{token}/sendMessage",
            {"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        )

    return send


def _post_json(url: str, payload: dict) -> None:
    data = json.dumps(payload).encode()
    request = Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        response.read()


# name, factory. factory() -> send(text) or None.
_CHANNELS = (
    ("email", _email_channel),
    ("slack", _slack_channel),
    ("telegram", _telegram_channel),
)
