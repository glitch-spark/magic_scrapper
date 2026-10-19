"""Administrator alerts go out once, on the channels that are configured."""
import json
from email.mime.text import MIMEText
from unittest.mock import MagicMock, patch

from utils import alerts


def _reset():
    alerts._sent.clear()


def test_unconfigured_alert_is_logged_once():
    _reset()
    with patch.object(alerts, "_open_channels", return_value=[]):
        alerts.alert("action", "login_halt", "sign in", source="builtin")
        alerts.alert("action", "login_halt", "sign in again", source="builtin")
    assert ("action", "login_halt", "builtin") in alerts._sent


def test_same_incident_is_sent_once():
    _reset()
    send = MagicMock()
    with patch.object(alerts, "_open_channels", return_value=[("slack", send)]):
        alerts.alert("info", "report", "builtin finished", source="builtin")
        alerts.alert("info", "report", "builtin finished again", source="builtin")
    send.assert_called_once()
    assert send.call_args.args[0].startswith("INFO\n")
    assert "report (builtin)" in send.call_args.args[0]


def test_failed_delivery_can_be_retried():
    _reset()
    send = MagicMock(side_effect=RuntimeError("down"))
    with patch.object(alerts, "_open_channels", return_value=[("slack", send)]):
        alerts.alert("action", "login_halt", "sign in", source="builtin")
    assert ("action", "login_halt", "builtin") not in alerts._sent


def test_alert_channels_limits_delivery(monkeypatch):
    _reset()
    monkeypatch.setenv("ALERT_CHANNELS", "slack")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/a")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    posted = []

    def capture(url, payload):
        posted.append((url, payload))

    with patch.object(alerts, "_post_json", side_effect=capture):
        alerts.alert("action", "login_halt", "captcha", source="flexjobs")
    assert len(posted) == 1
    assert posted[0][0] == "https://hooks.slack.example/a"
    assert "captcha" in posted[0][1]["text"]


def test_slack_and_telegram_payloads(monkeypatch):
    _reset()
    monkeypatch.setenv("ALERT_CHANNELS", "slack,telegram")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/a")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "42")
    posted = []

    def capture(url, payload):
        posted.append((url, payload))

    with patch.object(alerts, "_post_json", side_effect=capture):
        alerts.alert("action", "login_halt", "http://user:secret@proxy:1 timed out", source="builtin")
    urls = [url for url, _payload in posted]
    assert "https://hooks.slack.example/a" in urls
    assert "https://api.telegram.org/botsecret-token/sendMessage" in urls
    bodies = [payload["text"] for _url, payload in posted]
    assert all("secret@" not in body for body in bodies)
    assert all(body.startswith("NEED ACTION\n") for body in bodies)
    assert all("login_halt (builtin)" in body for body in bodies)
    telegram = next(payload for url, payload in posted if "telegram" in url)
    assert telegram["chat_id"] == "42"


def test_email_channel_sends_plain_text(monkeypatch):
    _reset()
    monkeypatch.setenv("ALERT_CHANNELS", "email")
    monkeypatch.setenv("ALERT_EMAIL_TO", "admin@example.com")
    server = MagicMock()
    with patch(
        "utils.email_report._get_credential",
        side_effect=lambda key: {
            "EMAIL_FROM": "bot@example.com",
            "EMAIL_TO": "other@example.com",
            "EMAIL_PASSWORD": "pw",
            "EMAIL_SMTP_HOST": "smtp.example.com",
            "EMAIL_SMTP_PORT": "587",
        }[key],
    ), patch("utils.alerts.smtplib.SMTP") as smtp:
        smtp.return_value.__enter__.return_value = server
        alerts.alert("info", "report", "fetch finished", source="builtin")
    server.login.assert_called_once_with("bot@example.com", "pw")
    raw = server.sendmail.call_args.args[2]
    assert "admin@example.com" in raw
    assert "Job Apply Agent INFO" in raw
    assert "fetch finished" in raw
    assert isinstance(MIMEText("x"), MIMEText)


def test_custom_channel_is_selected_by_name(monkeypatch):
    _reset()
    monkeypatch.setenv("ALERT_CHANNELS", "custom")
    seen = []
    monkeypatch.setattr(
        alerts,
        "_CHANNELS",
        (("custom", lambda: seen.append), ("slack", lambda: None)),
    )
    alerts.alert("action", "db_write", "mongo down", source="mongodb")
    assert seen == [
        "NEED ACTION\nJob Apply Agent needs attention\ndb_write (mongodb)\nmongo down"
    ]


def test_warning_is_not_sent():
    _reset()
    send = MagicMock()
    with patch.object(alerts, "_open_channels", return_value=[("slack", send)]):
        alerts.alert("warning", "fetch", "page skipped", source="builtin")
    send.assert_not_called()
    assert alerts._sent == set()


def test_db_write_error_recognizes_sqlite_lock():
    locked = RuntimeError("database is locked")
    assert alerts.is_skippable_db_lock(locked)
    assert alerts.is_db_write_error(locked)
    assert not alerts.is_db_write_error(RuntimeError("captcha"))


def test_halt_sends_action_and_stops():
    _reset()
    send = MagicMock()
    with patch.object(alerts, "_open_channels", return_value=[("slack", send)]):
        try:
            alerts.halt("login_halt", "signed out", source="builtin")
        except alerts.ScrapeHalted as exc:
            assert "signed out" in str(exc)
        else:
            raise AssertionError("halt did not stop the scrape")
    assert send.call_args.args[0].startswith("NEED ACTION\n")
    try:
        alerts.halt("login_halt", "signed out again", source="builtin")
    except alerts.ScrapeHalted:
        pass
    send.assert_called_once()


def test_post_json_posts_body():
    captured = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b""

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode())
        return _Response()

    with patch("utils.alerts.urlopen", side_effect=fake_urlopen):
        alerts._post_json("https://hooks.slack.example/a", {"text": "hi"})
    assert captured["body"] == {"text": "hi"}
    assert captured["timeout"] == 10
