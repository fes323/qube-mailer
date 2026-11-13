from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.presets import SITE_REPLY_ROUTES, seed_presets
from app.queue_crypto import EncryptedPayload

DEFAULT_EMAIL_TEMPLATE = """\
<!doctype html>
<html lang="ru">
  <body style="font-family:Arial,sans-serif;color:#172033;margin:0;padding:24px;background:#f4f6fa">
    <div style="max-width:680px;margin:auto;background:#fff;border-radius:12px;padding:28px">
      <h1 style="font-size:22px;margin:0 0 20px">Новая заявка</h1>
      {{ fields_table }}
      <p style="font-size:12px;color:#6b7280;margin-top:24px">Получено: {{ received_at }}</p>
    </div>
  </body>
</html>
"""

DEFAULT_SETTINGS = {
    "recipients": "",
    "sender_email": "",
    "sender_name": "Tilda",
    "subject_template": "Новая заявка с сайта — {{ formid }}",
    "email_template": DEFAULT_EMAIL_TEMPLATE,
}

STAT_COLUMNS = {
    "received",
    "accepted",
    "sent",
    "failed",
    "unauthorized",
    "test_requests",
    "queue_full",
    "duplicates",
    "expired",
}

FIELD_CATALOG_MAX_ENTRIES = 5_000
CMS_FORMS_MAX_ENTRIES = 2_000
ANY_FORM = "*"
HISTORY_KINDS = {"notification", "autoreply", "test"}
HISTORY_STATUSES = {"queued", "retrying", "sent", "failed", "skipped", "expired"}

DEFAULT_AUTOREPLY_TEMPLATE = """\
<!doctype html>
<html lang="ru">
  <body style="margin:0;padding:0;background:#f2f2f2;font-family:Arial,Helvetica,sans-serif;color:#111">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f2f2f2">
      <tr><td align="center" style="padding:32px 12px">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0"
               style="width:100%;max-width:600px;background:#ffffff;border-top:8px solid #111">
          <tr><td style="padding:32px 32px 8px;font-size:13px;letter-spacing:.12em;text-transform:uppercase;color:#555">
            QUBE
          </td></tr>
          <tr><td style="padding:0 32px 16px;font-size:24px;line-height:1.3;font-weight:bold">
            Спасибо, {{ Name }}! Мы получили вашу заявку
          </td></tr>
          <tr><td style="padding:0 32px 24px;font-size:15px;line-height:1.6">
            Форма «{{ formname }}» отправлена. Мы свяжемся с вами в ближайшее время.
          </td></tr>
          <tr><td style="padding:0 32px 32px">{{ fields_table }}</td></tr>
          <tr><td style="padding:16px 32px;background:#111;color:#fff;font-size:12px">
            Это автоматическое письмо. Ответьте на него, если хотите что-то уточнить.
          </td></tr>
        </table>
      </td></tr>
    </table>
  </body>
</html>
"""


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    accepted: bool
    duplicate: bool = False
    full: bool = False


@dataclass(frozen=True, slots=True)
class OutboxItem:
    id: str
    dedupe_key: bytes
    encrypted: EncryptedPayload
    attempts: int
    expires_at: float


@dataclass(frozen=True, slots=True)
class QueueStats:
    pending: int
    retrying: int
    oldest_created_at: float | None


@dataclass(frozen=True, slots=True)
class MailRoute:
    id: int
    name: str
    match_field: str
    match_value: str
    recipients: str
    subject_template: str
    email_template: str
    enabled: bool
    sender_name: str = ""
    # Also send to the default recipients from the general settings («админы»).
    include_default: bool = False


@dataclass(frozen=True, slots=True)
class Autoreply:
    id: int
    name: str
    enabled: bool
    match_field: str
    match_value: str
    email_field: str
    sender_name: str
    reply_to: str
    subject_template: str
    html_template: str
    text_template: str

    @property
    def any_form(self) -> bool:
        return self.match_field == ANY_FORM


@dataclass(frozen=True, slots=True)
class Attachment:
    id: int
    autoreply_id: int
    filename: str
    content_type: str
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class HistoryPage:
    rows: list[dict[str, Any]]
    total: int


AUTOREPLY_COLUMNS = (
    "name", "enabled", "match_field", "match_value", "email_field", "sender_name",
    "reply_to", "subject_template", "html_template", "text_template",
)


def _matches(match_field: str, match_value: str, fields: Mapping[str, Any]) -> bool:
    if match_field not in fields:
        return False
    raw_value = fields[match_field]
    values = raw_value if isinstance(raw_value, list) else [raw_value]
    return any(str(value).strip() == match_value for value in values)


def _matches_any(match_field: str, match_values: frozenset[str], fields: Mapping[str, Any]) -> bool:
    return any(_matches(match_field, value, fields) for value in match_values)


class Database:
    def __init__(self, path: Path):
        self.path = path

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        day TEXT PRIMARY KEY,
                        received INTEGER NOT NULL DEFAULT 0,
                        accepted INTEGER NOT NULL DEFAULT 0,
                        sent INTEGER NOT NULL DEFAULT 0,
                        failed INTEGER NOT NULL DEFAULT 0,
                        unauthorized INTEGER NOT NULL DEFAULT 0,
                        test_requests INTEGER NOT NULL DEFAULT 0,
                        queue_full INTEGER NOT NULL DEFAULT 0,
                        duplicates INTEGER NOT NULL DEFAULT 0,
                        expired INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                self._migrate_stat_columns(connection)
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS delivery_events (
                        dedupe_key BLOB PRIMARY KEY,
                        state TEXT NOT NULL CHECK(state IN ('pending', 'sent', 'expired', 'corrupt')),
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        forget_at REAL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS outbox (
                        id TEXT PRIMARY KEY,
                        dedupe_key BLOB NOT NULL UNIQUE,
                        key_version INTEGER NOT NULL,
                        nonce BLOB NOT NULL,
                        ciphertext BLOB NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        available_at REAL NOT NULL,
                        lease_until REAL,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS outbox_due_idx ON outbox(available_at, lease_until, created_at)"
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS mail_routes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        match_field TEXT NOT NULL,
                        match_value TEXT NOT NULL,
                        recipients TEXT NOT NULL,
                        subject_template TEXT NOT NULL,
                        email_template TEXT NOT NULL,
                        enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        UNIQUE(match_field, match_value)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS form_field_catalog (
                        form_key TEXT NOT NULL,
                        form_id TEXT NOT NULL,
                        form_name TEXT NOT NULL,
                        field_name TEXT NOT NULL,
                        first_seen_at REAL NOT NULL,
                        last_seen_at REAL NOT NULL,
                        seen_count INTEGER NOT NULL DEFAULT 1,
                        PRIMARY KEY(form_key, field_name)
                    )
                    """
                )
                self._create_autoreply_tables(connection)
                route_columns = {row["name"] for row in connection.execute("PRAGMA table_info(mail_routes)")}
                if "sender_name" not in route_columns:
                    connection.execute("ALTER TABLE mail_routes ADD COLUMN sender_name TEXT NOT NULL DEFAULT ''")
                if "include_default" not in route_columns:
                    connection.execute(
                        "ALTER TABLE mail_routes ADD COLUMN include_default INTEGER NOT NULL DEFAULT 0"
                    )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS field_catalog_last_seen_idx "
                    "ON form_field_catalog(last_seen_at DESC)"
                )
                for key, value in DEFAULT_SETTINGS.items():
                    connection.execute(
                        "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                        (key, value),
                    )
                seed_presets(connection)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _create_autoreply_tables(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS autoreplies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                match_field TEXT NOT NULL,
                match_value TEXT NOT NULL,
                email_field TEXT NOT NULL DEFAULT '',
                sender_name TEXT NOT NULL DEFAULT '',
                reply_to TEXT NOT NULL DEFAULT '',
                subject_template TEXT NOT NULL,
                html_template TEXT NOT NULL,
                text_template TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(match_field, match_value)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS autoreply_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                autoreply_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                content_type TEXT NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                created_at REAL NOT NULL,
                deleted_at REAL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS autoreply_attachments_idx "
            "ON autoreply_attachments(autoreply_id, deleted_at)"
        )
        # Send history: metadata only. Field values and letter bodies are never stored.
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS mail_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                delivery_key TEXT UNIQUE,
                kind TEXT NOT NULL,
                rule_id INTEGER,
                rule_name TEXT NOT NULL DEFAULT '',
                recipient TEXT NOT NULL DEFAULT '',
                subject TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                attachments INTEGER NOT NULL DEFAULT 0,
                submission_ref TEXT NOT NULL DEFAULT '',
                form_key TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS mail_history_created_idx ON mail_history(created_at DESC)")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS mail_history_recipient_idx ON mail_history(recipient COLLATE NOCASE)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS mail_history_ref_idx ON mail_history(submission_ref)")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS mail_history_rule_idx ON mail_history(kind, rule_id, created_at DESC)"
        )
        # Forms published in QUBE CMS (pushed through /api/v1/forms): identifiers and titles only.
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS cms_forms (
                formid TEXT PRIMARY KEY,
                formname TEXT NOT NULL DEFAULT '',
                form_route TEXT NOT NULL DEFAULT '',
                kind_label TEXT NOT NULL DEFAULT '',
                page_url TEXT NOT NULL DEFAULT '',
                admin_url TEXT NOT NULL DEFAULT '',
                position INTEGER NOT NULL DEFAULT 0,
                synced_at REAL NOT NULL
            )
            """
        )

    @staticmethod
    def _migrate_stat_columns(connection: sqlite3.Connection) -> None:
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(daily_stats)")}
        for column in ("duplicates", "expired"):
            if column not in columns:
                connection.execute(f"ALTER TABLE daily_stats ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0")

    @staticmethod
    def _today() -> str:
        return datetime.now(UTC).date().isoformat()

    @classmethod
    def _increment_in_transaction(cls, connection: sqlite3.Connection, column: str, amount: int = 1) -> None:
        if column not in STAT_COLUMNS:
            raise ValueError(f"Unknown stat column: {column}")
        today = cls._today()
        connection.execute("INSERT OR IGNORE INTO daily_stats(day) VALUES (?)", (today,))
        connection.execute(
            f"UPDATE daily_stats SET {column} = {column} + ? WHERE day = ?",
            (amount, today),
        )

    def get_settings(self) -> dict[str, str]:
        with self._connect() as connection:
            rows = connection.execute("SELECT key, value FROM settings").fetchall()
        result = DEFAULT_SETTINGS.copy()
        result.update({row["key"]: row["value"] for row in rows})
        return result

    def save_settings(self, values: dict[str, str]) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for key in DEFAULT_SETTINGS:
                    if key in values:
                        connection.execute(
                            "INSERT INTO settings(key, value) VALUES (?, ?) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (key, values[key]),
                        )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def get_routes(self) -> list[MailRoute]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM mail_routes ORDER BY id").fetchall()
        return [
            MailRoute(
                id=int(row["id"]),
                name=row["name"],
                match_field=row["match_field"],
                match_value=row["match_value"],
                recipients=row["recipients"],
                subject_template=row["subject_template"],
                email_template=row["email_template"],
                enabled=bool(row["enabled"]),
                sender_name=row["sender_name"],
                include_default=bool(row["include_default"]),
            )
            for row in rows
        ]

    def matching_route(self, fields: Mapping[str, Any]) -> MailRoute | None:
        for route in self.get_routes():
            if route.enabled and _matches(route.match_field, route.match_value, fields):
                return route
        return None

    def get_route(self, route_id: int) -> MailRoute | None:
        return next((route for route in self.get_routes() if route.id == route_id), None)

    def create_route(
        self,
        *,
        name: str,
        match_field: str,
        match_value: str,
        recipients: str,
        subject_template: str,
        email_template: str,
        enabled: bool,
        sender_name: str = "",
        include_default: bool = False,
    ) -> int:
        now = time.time()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO mail_routes(
                        name, match_field, match_value, recipients, subject_template,
                        email_template, enabled, created_at, updated_at, sender_name, include_default
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        match_field,
                        match_value,
                        recipients,
                        subject_template,
                        email_template,
                        int(enabled),
                        now,
                        now,
                        sender_name,
                        int(include_default),
                    ),
                )
                return int(cursor.lastrowid)
        except sqlite3.IntegrityError as error:
            raise ValueError("Маршрут для такой пары поля и значения уже существует") from error

    def update_route(
        self,
        route_id: int,
        *,
        name: str,
        match_field: str,
        match_value: str,
        recipients: str,
        subject_template: str,
        email_template: str,
        enabled: bool,
        sender_name: str = "",
        include_default: bool = False,
    ) -> None:
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    UPDATE mail_routes
                    SET name=?, match_field=?, match_value=?, recipients=?,
                        subject_template=?, email_template=?, enabled=?, updated_at=?, sender_name=?,
                        include_default=?
                    WHERE id=?
                    """,
                    (
                        name,
                        match_field,
                        match_value,
                        recipients,
                        subject_template,
                        email_template,
                        int(enabled),
                        time.time(),
                        sender_name,
                        int(include_default),
                        route_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ValueError("Маршрут не найден")
        except sqlite3.IntegrityError as error:
            raise ValueError("Маршрут для такой пары поля и значения уже существует") from error

    def delete_route(self, route_id: int) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM mail_routes WHERE id=?", (route_id,))

    @staticmethod
    def _catalog_text(value: Any, limit: int) -> str:
        if isinstance(value, list):
            value = value[0] if value else ""
        text = "".join(character for character in str(value).strip() if character >= " " and character != "\x7f")
        return text[:limit]

    def observe_fields(self, fields: Mapping[str, Any]) -> None:
        """Persist form schema metadata only; field values are never written."""
        form_id = self._catalog_text(fields.get("formid", ""), 200)
        form_name = self._catalog_text(fields.get("formname", ""), 300)
        form_key = f"id:{form_id}" if form_id else f"name:{form_name}" if form_name else "unknown"
        field_names = {
            cleaned
            for field_name in fields
            if (cleaned := self._catalog_text(field_name, 200))
        }
        if not field_names:
            return

        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                total = int(connection.execute("SELECT COUNT(*) FROM form_field_catalog").fetchone()[0])
                existing = {
                    row["field_name"]
                    for row in connection.execute(
                        "SELECT field_name FROM form_field_catalog WHERE form_key=?",
                        (form_key,),
                    )
                }
                for field_name in sorted(field_names):
                    if field_name not in existing and total >= FIELD_CATALOG_MAX_ENTRIES:
                        continue
                    connection.execute(
                        """
                        INSERT INTO form_field_catalog(
                            form_key, form_id, form_name, field_name,
                            first_seen_at, last_seen_at, seen_count
                        ) VALUES (?, ?, ?, ?, ?, ?, 1)
                        ON CONFLICT(form_key, field_name) DO UPDATE SET
                            form_id=excluded.form_id,
                            form_name=excluded.form_name,
                            last_seen_at=excluded.last_seen_at,
                            seen_count=form_field_catalog.seen_count + 1
                        """,
                        (form_key, form_id, form_name, field_name, now, now),
                    )
                    if field_name not in existing:
                        existing.add(field_name)
                        total += 1
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def field_catalog(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT form_key, form_id, form_name, field_name,
                       first_seen_at, last_seen_at, seen_count
                FROM form_field_catalog
                ORDER BY last_seen_at DESC, field_name COLLATE NOCASE
                """
            ).fetchall()

        groups: dict[str, dict[str, Any]] = {}
        for row in rows:
            group = groups.setdefault(
                row["form_key"],
                {
                    "form_id": row["form_id"],
                    "form_name": row["form_name"],
                    "first_seen_at": float(row["first_seen_at"]),
                    "last_seen_at": float(row["last_seen_at"]),
                    "fields": [],
                },
            )
            group["first_seen_at"] = min(group["first_seen_at"], float(row["first_seen_at"]))
            group["last_seen_at"] = max(group["last_seen_at"], float(row["last_seen_at"]))
            group["fields"].append({"name": row["field_name"], "seen_count": int(row["seen_count"])})

        result = list(groups.values())
        result.sort(key=lambda group: group["last_seen_at"], reverse=True)
        for group in result:
            group["first_seen"] = datetime.fromtimestamp(group.pop("first_seen_at"), UTC).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
            group["last_seen"] = datetime.fromtimestamp(group.pop("last_seen_at"), UTC).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
        return result

    def increment(self, column: str, amount: int = 1) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._increment_in_transaction(connection, column, amount)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def enqueue(
        self,
        *,
        event_id: str,
        dedupe_key: bytes,
        encrypted: EncryptedPayload,
        max_pending: int,
        retention_seconds: int,
        dedupe_retention_seconds: int,
    ) -> EnqueueResult:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._expire_due(connection, now, dedupe_retention_seconds)
                connection.execute(
                    "DELETE FROM delivery_events WHERE state != 'pending' AND forget_at <= ?",
                    (now,),
                )
                self._increment_in_transaction(connection, "received")
                existing = connection.execute(
                    "SELECT state FROM delivery_events WHERE dedupe_key = ?",
                    (dedupe_key,),
                ).fetchone()
                if existing and existing["state"] in {"pending", "sent"}:
                    self._increment_in_transaction(connection, "duplicates")
                    connection.execute("COMMIT")
                    return EnqueueResult(accepted=True, duplicate=True)
                if existing:
                    connection.execute("DELETE FROM delivery_events WHERE dedupe_key = ?", (dedupe_key,))

                pending = int(connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])
                if pending >= max_pending:
                    self._increment_in_transaction(connection, "queue_full")
                    connection.execute("COMMIT")
                    return EnqueueResult(accepted=False, full=True)

                connection.execute(
                    """
                    INSERT INTO delivery_events(dedupe_key, state, created_at, updated_at, forget_at)
                    VALUES (?, 'pending', ?, ?, NULL)
                    """,
                    (dedupe_key, now, now),
                )
                connection.execute(
                    """
                    INSERT INTO outbox(
                        id, dedupe_key, key_version, nonce, ciphertext, attempts,
                        available_at, lease_until, created_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, NULL, ?, ?)
                    """,
                    (
                        event_id,
                        dedupe_key,
                        encrypted.key_version,
                        encrypted.nonce,
                        encrypted.ciphertext,
                        now,
                        now,
                        now + retention_seconds,
                    ),
                )
                self._increment_in_transaction(connection, "accepted")
                connection.execute("COMMIT")
                return EnqueueResult(accepted=True)
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def _expire_due(self, connection: sqlite3.Connection, now: float, dedupe_retention_seconds: int) -> None:
        rows = connection.execute(
            "SELECT dedupe_key FROM outbox WHERE expires_at <= ?",
            (now,),
        ).fetchall()
        if not rows:
            return
        keys = [row["dedupe_key"] for row in rows]
        connection.executemany(
            "UPDATE delivery_events SET state='expired', updated_at=?, forget_at=? WHERE dedupe_key=?",
            [(now, now + dedupe_retention_seconds, key) for key in keys],
        )
        connection.execute("DELETE FROM outbox WHERE expires_at <= ?", (now,))
        self._increment_in_transaction(connection, "expired", len(keys))

    def claim_next(self, lease_seconds: int, dedupe_retention_seconds: int) -> OutboxItem | None:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._expire_due(connection, now, dedupe_retention_seconds)
                row = connection.execute(
                    """
                    SELECT * FROM outbox
                    WHERE available_at <= ? AND (lease_until IS NULL OR lease_until <= ?)
                    ORDER BY created_at
                    LIMIT 1
                    """,
                    (now, now),
                ).fetchone()
                if row is None:
                    connection.execute("COMMIT")
                    return None
                attempts = int(row["attempts"]) + 1
                connection.execute(
                    "UPDATE outbox SET attempts=?, lease_until=? WHERE id=?",
                    (attempts, now + lease_seconds, row["id"]),
                )
                connection.execute("COMMIT")
                return OutboxItem(
                    id=row["id"],
                    dedupe_key=row["dedupe_key"],
                    encrypted=EncryptedPayload(
                        key_version=int(row["key_version"]),
                        nonce=row["nonce"],
                        ciphertext=row["ciphertext"],
                    ),
                    attempts=attempts,
                    expires_at=float(row["expires_at"]),
                )
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def save_delivery_progress(self, item: OutboxItem, encrypted: EncryptedPayload, lease_seconds: int) -> None:
        """Checkpoint pending recipients inside the ciphertext, renewing ownership of the lease."""
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE outbox SET key_version=?, nonce=?, ciphertext=?, lease_until=? "
                "WHERE id=? AND attempts=?",
                (encrypted.key_version, encrypted.nonce, encrypted.ciphertext,
                 time.time() + lease_seconds, item.id, item.attempts),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Outbox lease was lost")

    def mark_sent(self, item: OutboxItem, dedupe_retention_seconds: int) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM outbox WHERE id = ?", (item.id,))
                connection.execute(
                    "UPDATE delivery_events SET state='sent', updated_at=?, forget_at=? WHERE dedupe_key=?",
                    (now, now + dedupe_retention_seconds, item.dedupe_key),
                )
                self._increment_in_transaction(connection, "sent")
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def reschedule(self, item: OutboxItem, delay_seconds: int, dedupe_retention_seconds: int) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if item.expires_at <= now:
                    self._expire_due(connection, now, dedupe_retention_seconds)
                else:
                    connection.execute(
                        "UPDATE outbox SET available_at=?, lease_until=NULL WHERE id=?",
                        (min(now + delay_seconds, item.expires_at), item.id),
                    )
                    self._increment_in_transaction(connection, "failed")
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def discard_corrupt(self, item: OutboxItem, dedupe_retention_seconds: int) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM outbox WHERE id=?", (item.id,))
                connection.execute(
                    "UPDATE delivery_events SET state='corrupt', updated_at=?, forget_at=? WHERE dedupe_key=?",
                    (now, now + dedupe_retention_seconds, item.dedupe_key),
                )
                self._increment_in_transaction(connection, "expired")
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def queue_stats(self) -> QueueStats:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) pending,
                       COALESCE(SUM(CASE WHEN attempts > 0 THEN 1 ELSE 0 END), 0) retrying,
                       MIN(created_at) oldest_created_at
                FROM outbox
                """
            ).fetchone()
        return QueueStats(int(row["pending"]), int(row["retrying"]), row["oldest_created_at"])

    def pending_key_versions(self) -> set[int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT DISTINCT key_version FROM outbox").fetchall()
        return {int(row["key_version"]) for row in rows}

    def stats(self, days: int = 14) -> tuple[dict[str, int], list[dict[str, int | str]]]:
        first_day = (datetime.now(UTC).date() - timedelta(days=days - 1)).isoformat()
        columns = ", ".join(f"COALESCE(SUM({column}), 0) {column}" for column in sorted(STAT_COLUMNS))
        with self._connect() as connection:
            totals_row = connection.execute(f"SELECT {columns} FROM daily_stats").fetchone()
            rows = connection.execute(
                "SELECT * FROM daily_stats WHERE day >= ? ORDER BY day",
                (first_day,),
            ).fetchall()
        totals = {key: int(totals_row[key]) for key in STAT_COLUMNS}
        return totals, [dict(row) for row in rows]

    # --- Autoreplies («отбивки») -------------------------------------------------

    @staticmethod
    def _autoreply(row: sqlite3.Row) -> Autoreply:
        return Autoreply(
            id=int(row["id"]), name=row["name"], enabled=bool(row["enabled"]),
            match_field=row["match_field"], match_value=row["match_value"],
            email_field=row["email_field"], sender_name=row["sender_name"], reply_to=row["reply_to"],
            subject_template=row["subject_template"], html_template=row["html_template"],
            text_template=row["text_template"],
        )

    def get_autoreplies(self, *, limit: int = 1000, offset: int = 0) -> list[Autoreply]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM autoreplies ORDER BY id LIMIT ? OFFSET ?", (limit, offset)
            ).fetchall()
        return [self._autoreply(row) for row in rows]

    def count_autoreplies(self) -> int:
        with self._connect() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM autoreplies").fetchone()[0])

    def get_autoreply(self, autoreply_id: int) -> Autoreply | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM autoreplies WHERE id=?", (autoreply_id,)).fetchone()
        return self._autoreply(row) if row else None

    def matching_autoreply(self, fields: Mapping[str, Any]) -> Autoreply | None:
        """A rule for this exact form wins; a rule for «all forms» is the fallback.

        The fallback skips forms whose submitter already gets a letter from the site itself.
        """
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM autoreplies WHERE enabled=1 ORDER BY id").fetchall()
        rules = [self._autoreply(row) for row in rows]
        for rule in rules:
            if not rule.any_form and _matches(rule.match_field, rule.match_value, fields):
                return rule
        if _matches_any("form_route", SITE_REPLY_ROUTES, fields):
            return None
        return next((rule for rule in rules if rule.any_form), None)

    def save_autoreply(self, autoreply_id: int | None, values: Mapping[str, Any]) -> int:
        now = time.time()
        params = [int(values[key]) if key == "enabled" else values[key] for key in AUTOREPLY_COLUMNS]
        try:
            with self._connect() as connection:
                if autoreply_id is None:
                    placeholders = ", ".join("?" for _ in AUTOREPLY_COLUMNS)
                    cursor = connection.execute(
                        f"INSERT INTO autoreplies({', '.join(AUTOREPLY_COLUMNS)}, created_at, updated_at) "
                        f"VALUES ({placeholders}, ?, ?)",
                        (*params, now, now),
                    )
                    return int(cursor.lastrowid)
                assignments = ", ".join(f"{key}=?" for key in AUTOREPLY_COLUMNS)
                cursor = connection.execute(
                    f"UPDATE autoreplies SET {assignments}, updated_at=? WHERE id=?",
                    (*params, now, autoreply_id),
                )
                if cursor.rowcount != 1:
                    raise ValueError("Отбивка не найдена")
                return autoreply_id
        except sqlite3.IntegrityError as error:
            raise ValueError("Отбивка для такой формы уже существует") from error

    def delete_autoreply(self, autoreply_id: int) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM autoreplies WHERE id=?", (autoreply_id,))
                # Files stay until queued letters that reference them have expired.
                connection.execute(
                    "UPDATE autoreply_attachments SET deleted_at=? WHERE autoreply_id=? AND deleted_at IS NULL",
                    (now, autoreply_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    # --- Attachments: content-addressed files next to the database ---------------

    @property
    def attachments_dir(self) -> Path:
        return self.path.parent / "attachments"

    def attachment_path(self, sha256: str) -> Path:
        if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
            raise ValueError("Invalid attachment reference")
        return self.attachments_dir / sha256

    def add_attachment(self, autoreply_id: int, filename: str, content_type: str, data: bytes) -> Attachment:
        digest = hashlib.sha256(data).hexdigest()
        target = self.attachment_path(digest)
        if not target.exists():
            self.attachments_dir.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{digest}.{os.getpid()}.tmp")
            temporary.write_bytes(data)
            os.replace(temporary, target)
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO autoreply_attachments(autoreply_id, filename, content_type, size, sha256, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (autoreply_id, filename, content_type, len(data), digest, now),
            )
        return Attachment(int(cursor.lastrowid), autoreply_id, filename, content_type, len(data), digest)

    def get_attachments(self, autoreply_id: int) -> list[Attachment]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM autoreply_attachments WHERE autoreply_id=? AND deleted_at IS NULL ORDER BY id",
                (autoreply_id,),
            ).fetchall()
        return [
            Attachment(int(row["id"]), int(row["autoreply_id"]), row["filename"], row["content_type"],
                       int(row["size"]), row["sha256"])
            for row in rows
        ]

    def attachment_counts(self) -> dict[int, int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT autoreply_id, COUNT(*) total FROM autoreply_attachments "
                "WHERE deleted_at IS NULL GROUP BY autoreply_id"
            ).fetchall()
        return {int(row["autoreply_id"]): int(row["total"]) for row in rows}

    def remove_attachments(self, autoreply_id: int, attachment_ids: list[int]) -> None:
        if not attachment_ids:
            return
        with self._connect() as connection:
            connection.executemany(
                "UPDATE autoreply_attachments SET deleted_at=? WHERE id=? AND autoreply_id=? AND deleted_at IS NULL",
                [(time.time(), attachment_id, autoreply_id) for attachment_id in attachment_ids],
            )

    def purge_attachments(self, keep_seconds: int) -> int:
        """Forget removed attachments once no queued letter can still reference them."""
        cutoff = time.time() - keep_seconds
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                candidates = {
                    row["sha256"]
                    for row in connection.execute(
                        "SELECT sha256 FROM autoreply_attachments WHERE deleted_at IS NOT NULL AND deleted_at <= ?",
                        (cutoff,),
                    )
                }
                connection.execute(
                    "DELETE FROM autoreply_attachments WHERE deleted_at IS NOT NULL AND deleted_at <= ?", (cutoff,)
                )
                in_use = {
                    row["sha256"]
                    for row in connection.execute("SELECT DISTINCT sha256 FROM autoreply_attachments")
                }
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        removed = 0
        for digest in candidates - in_use:
            try:
                self.attachment_path(digest).unlink()
                removed += 1
            except FileNotFoundError:
                pass
        return removed

    # --- Send history --------------------------------------------------------------

    def history_open(
        self, *, delivery_key: str | None, kind: str, recipient: str, subject: str = "",
        rule_id: int | None = None, rule_name: str = "", status: str = "queued", error: str = "",
        attachments: int = 0, submission_ref: str = "", form_key: str = "",
    ) -> int:
        """Idempotent per delivery_key: a replanned delivery reuses its rows."""
        if kind not in HISTORY_KINDS or status not in HISTORY_STATUSES:
            raise ValueError("Unknown history kind or status")
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO mail_history(
                    delivery_key, kind, rule_id, rule_name, recipient, subject, status, error,
                    attachments, submission_ref, form_key, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (delivery_key, kind, rule_id, rule_name[:200], recipient[:320], subject[:250], status,
                 error[:120], attachments, submission_ref, form_key[:300], now, now),
            )
            if cursor.rowcount == 1:
                return int(cursor.lastrowid)
            row = connection.execute(
                "SELECT id FROM mail_history WHERE delivery_key=?", (delivery_key,)
            ).fetchone()
            return int(row["id"])

    def history_update(self, history_id: int | None, *, status: str, error: str = "", subject: str | None = None) -> None:
        if history_id is None:
            return
        if status not in HISTORY_STATUSES:
            raise ValueError("Unknown history status")
        with self._connect() as connection:
            connection.execute(
                "UPDATE mail_history SET status=?, error=?, attempts=attempts+1, updated_at=?, "
                "subject=COALESCE(?, subject) WHERE id=?",
                (status, error[:120], time.time(), subject[:250] if subject is not None else None, history_id),
            )

    def history_page(
        self, *, kind: str = "", status: str = "", rule_id: int | None = None, recipient: str = "",
        submission_ref: str = "", start: float | None = None, end: float | None = None,
        limit: int = 50, offset: int = 0,
    ) -> HistoryPage:
        clauses: list[str] = []
        params: list[Any] = []
        if kind:
            clauses.append("kind=?")
            params.append(kind)
        if status:
            clauses.append("status=?")
            params.append(status)
        if rule_id is not None:
            clauses.append("rule_id=? AND kind IN ('autoreply', 'test')")
            params.append(rule_id)
        if recipient:
            escaped = recipient.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("recipient LIKE ? ESCAPE '\\'")
            params.append(escaped + "%")
        if submission_ref:
            clauses.append("submission_ref=?")
            params.append(submission_ref)
        if start is not None:
            clauses.append("created_at>=?")
            params.append(start)
        if end is not None:
            clauses.append("created_at<?")
            params.append(end)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as connection:
            total = int(connection.execute(f"SELECT COUNT(*) FROM mail_history {where}", params).fetchone()[0])
            rows = connection.execute(
                f"SELECT * FROM mail_history {where} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item.pop("delivery_key", None)
            item["created"] = datetime.fromtimestamp(item["created_at"], UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
            item["updated"] = datetime.fromtimestamp(item["updated_at"], UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
            result.append(item)
        return HistoryPage(result, total)

    def autoreply_sent_counts(self, days: int = 30) -> dict[int, int]:
        since = time.time() - days * 86400
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT rule_id, COUNT(*) total FROM mail_history "
                "WHERE kind='autoreply' AND status='sent' AND created_at>=? GROUP BY rule_id",
                (since,),
            ).fetchall()
        return {int(row["rule_id"]): int(row["total"]) for row in rows if row["rule_id"] is not None}

    def purge_history(self, *, retention_seconds: int, queue_retention_seconds: int) -> None:
        now = time.time()
        with self._connect() as connection:
            # Letters still unsent after the queue TTL were dropped together with the ciphertext.
            connection.execute(
                "UPDATE mail_history SET status='expired', updated_at=? "
                "WHERE status IN ('queued', 'retrying') AND created_at<=?",
                (now, now - queue_retention_seconds - 3600),
            )
            connection.execute("DELETE FROM mail_history WHERE created_at<=?", (now - retention_seconds,))

    # --- Forms catalogue pushed by QUBE CMS --------------------------------------

    def replace_cms_forms(self, forms: list[dict[str, str]]) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM cms_forms")
                connection.executemany(
                    "INSERT OR REPLACE INTO cms_forms(formid, formname, form_route, kind_label, page_url, "
                    "admin_url, position, synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (form["formid"], form["formname"], form["form_route"], form["kind_label"],
                         form["page_url"], form["admin_url"], position, now)
                        for position, form in enumerate(forms)
                    ],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def cms_forms(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM cms_forms ORDER BY position").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["synced"] = datetime.fromtimestamp(item["synced_at"], UTC).strftime("%Y-%m-%d %H:%M UTC")
            result.append(item)
        return result

    def known_form_values(self) -> dict[str, list[str]]:
        """Suggestions for the «field = value» condition: formid, formname and form_route."""
        values: dict[str, set[str]] = {"formid": set(), "formname": set(), "form_route": set()}
        with self._connect() as connection:
            for row in connection.execute("SELECT formid, formname, form_route FROM cms_forms"):
                for key in values:
                    if row[key]:
                        values[key].add(row[key])
            for row in connection.execute("SELECT DISTINCT form_id, form_name FROM form_field_catalog"):
                if row["form_id"]:
                    values["formid"].add(row["form_id"])
                if row["form_name"]:
                    values["formname"].add(row["form_name"])
        return {key: sorted(items) for key, items in values.items()}

    def form_field_names(self, match_field: str, match_value: str) -> list[str]:
        """Variable names seen for one form; used for previews and test letters."""
        with self._connect() as connection:
            if match_field == "formid":
                rows = connection.execute(
                    "SELECT DISTINCT field_name FROM form_field_catalog WHERE form_id=? ORDER BY field_name",
                    (match_value,),
                ).fetchall()
            elif match_field == "formname":
                rows = connection.execute(
                    "SELECT DISTINCT field_name FROM form_field_catalog WHERE form_name=? ORDER BY field_name",
                    (match_value,),
                ).fetchall()
            else:
                rows = []
        return [row["field_name"] for row in rows]
