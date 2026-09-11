from __future__ import annotations

import os
import re
import sqlite3
import tempfile
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

os.environ.setdefault("WEBHOOK_API_KEY", "module-test-key-at-least-32-characters")
os.environ.setdefault("ADMIN_PASSWORD", "module-test-password-at-least-32-chars")
os.environ.setdefault("CSRF_SECRET", "module-test-csrf-at-least-32-characters")
os.environ.setdefault("DATABASE_PATH", str(Path(tempfile.gettempdir()) / "tilda-webhook-import-test.db"))
os.environ.setdefault("QUEUE_KEYRING_FILE", str(Path(__file__).parent / "fixtures" / "queue-keyring.json"))

from app.config import Config
from app.database import Database
from app.main import create_app
from app.mailer import MailWorker, Submission
from app.queue_crypto import QueueCipher
from app.templating import render_html, render_subject


def config(tmp_path: Path) -> Config:
    return Config(
        database_path=tmp_path / "test.db",
        webhook_api_key="webhook-secret-at-least-32-characters",
        webhook_api_key_header="X-Webhook-Key",
        admin_username="admin",
        admin_password="admin-secret-at-least-32-characters",
        csrf_secret="csrf-secret-at-least-32-characters",
        smtp_host="",
        smtp_port=587,
        smtp_username="",
        smtp_password="",
        smtp_security="starttls",
        smtp_timeout_seconds=1,
        queue_size=10,
        max_body_bytes=4096,
        queue_keys={1: b"e" * 32},
        active_queue_key_version=1,
        queue_dedupe_key=b"d" * 32,
        queue_retention_seconds=604800,
        dedupe_retention_seconds=604800,
        queue_lease_seconds=300,
        retry_base_seconds=5,
        retry_max_seconds=3600,
    )


def test_config_rejects_weak_or_reused_secrets(tmp_path: Path) -> None:
    values = config(tmp_path)
    weak = Config(
        **{field: getattr(values, field) for field in values.__dataclass_fields__ if field != "admin_password"},
        admin_password="short",
    )
    try:
        weak.validate()
    except RuntimeError as error:
        assert "ADMIN_PASSWORD" in str(error)
    else:
        raise AssertionError("Weak password must be rejected")


def test_fail2ban_filter_matches_only_failed_admin_auth() -> None:
    filter_text = Path("deploy/fail2ban/filter.d/tilda-admin.conf").read_text(encoding="utf-8")
    failregex = next(line.split("=", 1)[1].strip() for line in filter_text.splitlines() if line.startswith("failregex"))
    pattern = re.compile(failregex.replace("<HOST>", r"(?P<host>(?:\d{1,3}\.){3}\d{1,3})"))
    base = (
        '{{"level":"info","ts":1787200000.0,"logger":"http.log.access","msg":"handled request",'
        '"request":{{"remote_ip":"203.0.113.7","remote_port":"45678","client_ip":"203.0.113.7",'
        '"proto":"HTTP/2.0","method":"GET","host":"example.test","uri":"{uri}"}},'
        '"bytes_read":0,"duration":0.001,"size":0,"status":{status}}}'
    )
    assert pattern.match(base.format(uri="/admin", status=401)).group("host") == "203.0.113.7"
    assert pattern.match(base.format(uri="/admin/settings", status=401))
    assert pattern.match(base.format(uri="/admin/routes", status=401))
    assert pattern.match(base.format(uri="/admin/routes/12", status=401))
    assert pattern.match(base.format(uri="/admin/logs?offset=50", status=401))
    assert not pattern.match(base.format(uri="/administrator", status=401))
    assert not pattern.match(base.format(uri="/admin", status=200))
    assert not pattern.match(base.format(uri="/webhook/tilda", status=401))


def test_webhook_auth_test_handshake_and_queue(tmp_path: Path) -> None:
    app = create_app(config(tmp_path))
    with TestClient(app) as client:
        assert client.post("/webhook/tilda", data={"Name": "Иван"}).status_code == 401
        response = client.post(
            "/webhook/tilda",
            data={"test": "test"},
            headers={"X-Webhook-Key": "webhook-secret-at-least-32-characters"},
        )
        assert response.status_code == 200
        assert response.text == "ok"

        response = client.post(
            "/webhook/tilda",
            data={"Name": "Иван", "Phone": "+79990000000", "tranid": "1:2", "formid": "form1"},
            headers={"X-Webhook-Key": "webhook-secret-at-least-32-characters"},
        )
        assert response.status_code == 200
        assert app.state.database.queue_stats().pending == 1

        duplicate = client.post(
            "/webhook/tilda",
            data={"Name": "Иван", "Phone": "+79990000000", "tranid": "1:2", "formid": "form1"},
            headers={"X-Webhook-Key": "webhook-secret-at-least-32-characters"},
        )
        assert duplicate.status_code == 200
        assert app.state.database.queue_stats().pending == 1
        totals, _ = app.state.database.stats()
        assert totals["duplicates"] == 1


def test_search_indexing_is_disabled_for_all_routes(tmp_path: Path) -> None:
    app = create_app(config(tmp_path))
    with TestClient(app) as client:
        robots = client.get("/robots.txt")
        assert robots.status_code == 200
        assert robots.text == "User-agent: *\nDisallow: /\n"
        assert robots.headers["x-robots-tag"] == "noindex, nofollow, noarchive, nosnippet, noimageindex"

        health = client.get("/health/live")
        assert health.headers["x-robots-tag"] == "noindex, nofollow, noarchive, nosnippet, noimageindex"

        admin = client.get("/admin", auth=("admin", "admin-secret-at-least-32-characters"))
        assert '<meta name="robots" content="noindex,nofollow,noarchive,nosnippet,noimageindex">' in admin.text


def test_database_does_not_persist_lead_fields(tmp_path: Path) -> None:
    app = create_app(config(tmp_path))
    with TestClient(app) as client:
        client.post(
            "/webhook/tilda",
            data={"Email": "private@example.test", "Phone": "+79990000000", "tranid": "secret-lead-id"},
            headers={"X-Webhook-Key": "webhook-secret-at-least-32-characters"},
        )
    with sqlite3.connect(tmp_path / "test.db") as connection:
        dump = "\n".join(connection.iterdump())
    assert "private@example.test" not in dump
    assert "+79990000000" not in dump


def test_encrypted_outbox_survives_restart_and_expired_lease(tmp_path: Path) -> None:
    values = config(tmp_path)
    database = Database(values.database_path)
    database.initialize()
    cipher = QueueCipher(values.queue_keys, values.active_queue_key_version, values.queue_dedupe_key)
    fields = {
        "Name": "Секретное имя",
        "Phone": "+79991112233",
        "tranid": "100:200",
        "formid": "form42",
    }
    event_id = "event-before-crash"
    dedupe_key = cipher.dedupe(fields, event_id)
    encrypted = cipher.encrypt(event_id, fields, datetime.now(UTC))
    result = database.enqueue(
        event_id=event_id,
        dedupe_key=dedupe_key,
        encrypted=encrypted,
        max_pending=10,
        retention_seconds=3600,
        dedupe_retention_seconds=3600,
    )
    assert result.accepted and not result.duplicate

    claimed_before_crash = database.claim_next(300, 3600)
    assert claimed_before_crash is not None

    restarted_database = Database(values.database_path)
    restarted_database.initialize()
    assert restarted_database.claim_next(300, 3600) is None
    with sqlite3.connect(values.database_path) as connection:
        connection.execute("UPDATE outbox SET lease_until=0")

    recovered = restarted_database.claim_next(300, 3600)
    assert recovered is not None
    recovered_fields, _ = cipher.decrypt(recovered.id, recovered.encrypted)
    assert recovered_fields == fields
    restarted_database.mark_sent(recovered, 3600)
    assert restarted_database.queue_stats().pending == 0

    duplicate = restarted_database.enqueue(
        event_id="retry-after-success",
        dedupe_key=cipher.dedupe(fields, "retry-after-success"),
        encrypted=cipher.encrypt("retry-after-success", fields, datetime.now(UTC)),
        max_pending=10,
        retention_seconds=3600,
        dedupe_retention_seconds=3600,
    )
    assert duplicate.duplicate
    assert restarted_database.queue_stats().pending == 0


def test_template_escapes_values_and_blocks_subject_injection() -> None:
    fields = {
        "Name": "<script>x</script>",
        "Имя клиента": "Иван",
        "utm source": "catalog",
        "received_at": "значение поля",
    }
    result = render_html(
        "{{ Name }} {{ Имя клиента }} {{ utm source }} {{ field:received_at }} {{ fields_table }}",
        fields,
        "now",
    )
    assert "<script>" not in result
    assert "&lt;script&gt;" in result
    assert "Иван" in result
    assert "catalog" in result
    assert "значение поля" in result
    assert render_subject("Заявка {{ Name }}", {"Name": "A\r\nBcc: bad@example.test"}) == "Заявка A Bcc: bad@example.test"


def test_form_routes_and_field_catalog_store_metadata_only(tmp_path: Path) -> None:
    database = Database(tmp_path / "routes.db")
    database.initialize()
    first = {
        "formid": "form-x",
        "formname": "Форма X",
        "Имя клиента": "Пётр Секретов",
        "Email": "private@example.test",
    }
    second = {**first, "Имя клиента": "Другое имя", "Phone": "+79990000000"}
    database.observe_fields(first)
    database.observe_fields(second)
    database.create_route(
        name="Форма X → Петя",
        match_field="formname",
        match_value="Форма X",
        recipients="petya@example.test",
        subject_template="X: {{ Имя клиента }}",
        email_template="<p>{{ Имя клиента }}</p>",
        enabled=True,
    )

    route = database.matching_route(second)
    assert route is not None
    assert route.recipients == "petya@example.test"
    assert database.matching_route({"formname": "Форма Y"}) is None

    catalog = database.field_catalog()
    assert catalog[0]["form_id"] == "form-x"
    assert catalog[0]["form_name"] == "Форма X"
    fields = {entry["name"]: entry["seen_count"] for entry in catalog[0]["fields"]}
    assert fields["Имя клиента"] == 2
    assert fields["Phone"] == 1

    with sqlite3.connect(tmp_path / "routes.db") as connection:
        dump = "\n".join(connection.iterdump())
    assert "Пётр Секретов" not in dump
    assert "private@example.test" not in dump
    assert "+79990000000" not in dump


def test_mail_worker_uses_matching_route(tmp_path: Path, monkeypatch) -> None:
    values = replace(config(tmp_path), smtp_host="smtp.example.test")
    database = Database(values.database_path)
    database.initialize()
    database.save_settings(
        {
            "recipients": "default@example.test",
            "sender_email": "forms@example.test",
            "sender_name": "Forms",
            "subject_template": "Default",
            "email_template": "<p>Default</p>",
        }
    )
    database.create_route(
        name="Форма Y → Маша",
        match_field="formid",
        match_value="form-y",
        recipients="masha@example.test",
        subject_template="Y: {{ Проект }}",
        email_template="<strong>{{ Проект }}</strong>",
        enabled=True,
    )
    sent: list[tuple] = []

    class FakeSMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def starttls(self, **_kwargs):
            return None

        def login(self, *_args):
            return None

        def send_message(self, message, *, from_addr, to_addrs):
            sent.append((message, from_addr, to_addrs))
            return {}

    monkeypatch.setattr("app.mailer.smtplib.SMTP", FakeSMTP)
    cipher = QueueCipher(values.queue_keys, values.active_queue_key_version, values.queue_dedupe_key)
    worker = MailWorker(values, database, cipher)
    worker._send(  # noqa: SLF001 - verifies routing at the SMTP boundary
        Submission(fields={"formid": "form-y", "Проект": "Экспо"}, received_at=datetime.now(UTC)),
        b"r" * 32,
    )

    message, sender, recipients = sent[0]
    assert sender == "forms@example.test"
    assert recipients == ["masha@example.test"]
    assert message["To"] == "masha@example.test"
    assert message["Subject"] == "Y: Экспо"
    assert "<strong>Экспо</strong>" in message.get_body(preferencelist=("html",)).get_content()


def test_admin_auth_and_settings_save(tmp_path: Path) -> None:
    app = create_app(config(tmp_path))
    auth = ("admin", "admin-secret-at-least-32-characters")
    with TestClient(app) as client:
        assert client.get("/admin").status_code == 401
        page = client.get("/admin", auth=auth)
        assert page.status_code == 200
        assert "default-src 'none'" in page.headers["content-security-policy"]
        token = page.text.split('name="csrf" value="', 1)[1].split('"', 1)[0]
        response = client.post(
            "/admin/settings",
            auth=auth,
            data={
                "csrf": token,
                "recipients": "team@example.test",
                "sender_email": "forms@example.test",
                "sender_name": "Forms",
                "subject_template": "Lead {{ Name }}",
                "email_template": "<p>{{ fields_table }}</p>",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert app.state.database.get_settings()["recipients"] == "team@example.test"

        response = client.post(
            "/admin/routes",
            auth=auth,
            data={
                "csrf": token,
                "name": "Form X",
                "match_field": "formid",
                "match_value": "form-x",
                "recipients": "petya@example.test",
                "subject_template": "X {{ Name }}",
                "email_template": "<p>{{ Name }}</p>",
                "enabled": "1",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        routes = app.state.database.get_routes()
        assert len(routes) == 1
        assert routes[0].match_value == "form-x"
        assert "Form X" in client.get("/admin/routes", auth=auth).text
