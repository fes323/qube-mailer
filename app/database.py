from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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
                route_columns = {row["name"] for row in connection.execute("PRAGMA table_info(mail_routes)")}
                if "sender_name" not in route_columns:
                    connection.execute("ALTER TABLE mail_routes ADD COLUMN sender_name TEXT NOT NULL DEFAULT ''")
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS field_catalog_last_seen_idx "
                    "ON form_field_catalog(last_seen_at DESC)"
                )
                for key, value in DEFAULT_SETTINGS.items():
                    connection.execute(
                        "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                        (key, value),
                    )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

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
            )
            for row in rows
        ]

    def matching_route(self, fields: Mapping[str, Any]) -> MailRoute | None:
        for route in self.get_routes():
            if not route.enabled or route.match_field not in fields:
                continue
            raw_value = fields[route.match_field]
            values = raw_value if isinstance(raw_value, list) else [raw_value]
            if any(str(value).strip() == route.match_value for value in values):
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
    ) -> int:
        now = time.time()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO mail_routes(
                        name, match_field, match_value, recipients, subject_template,
                        email_template, enabled, created_at, updated_at, sender_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    ) -> None:
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    UPDATE mail_routes
                    SET name=?, match_field=?, match_value=?, recipients=?,
                        subject_template=?, email_template=?, enabled=?, updated_at=?, sender_name=?
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
