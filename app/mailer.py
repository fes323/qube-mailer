from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
import time
import uuid
import smtplib
import ssl
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from typing import Any

from app.config import Config
from app.database import Attachment, Autoreply, Database, OutboxItem
from app.queue_crypto import QueueCipher
from app.templating import html_to_text, render_html, render_subject, render_text

EMAIL_PATTERN = re.compile(r"[^@\s<>(),;:\"\[\]]{1,64}@[A-Za-z0-9.-]{1,252}\.[A-Za-z0-9-]{2,63}")
AUTO_EMAIL_FIELDS = ("email", "e-mail", "mail")


def parse_recipients(raw: str) -> list[str]:
    values = raw.replace(";", ",").replace("\n", ",").split(",")
    recipients: list[str] = []
    seen: set[str] = set()
    for value in values:
        address = value.strip()
        if not address:
            continue
        if "\r" in address or "\n" in address or address.count("@") != 1:
            raise ValueError(f"Некорректный адрес получателя: {address}")
        if address.casefold() not in seen:
            seen.add(address.casefold())
            recipients.append(address)
    if not recipients:
        raise ValueError("Укажите хотя бы одного получателя")
    return recipients


def safe_reply_to(fields: dict[str, Any]) -> str | None:
    for key, value in fields.items():
        if key.lower() in AUTO_EMAIL_FIELDS:
            candidate = str(value).strip()
            if "\r" not in candidate and "\n" not in candidate and candidate.count("@") == 1:
                return candidate
    return None


def valid_email(value: str) -> bool:
    return len(value) <= 254 and EMAIL_PATTERN.fullmatch(value) is not None


def submitter_email(fields: dict[str, Any], email_field: str = "") -> str | None:
    """The address of the person who filled in the form, or None when it is missing or invalid."""
    if email_field:
        candidates = [fields.get(email_field)]
    else:
        candidates = [value for key, value in fields.items() if key.lower() in AUTO_EMAIL_FIELDS]
    for raw in candidates:
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        candidate = str(raw or "").strip()
        if valid_email(candidate):
            return candidate
    return None


def error_code(error: BaseException) -> str:
    """A short diagnostic without server text: SMTP replies may echo addresses or content."""
    code = getattr(error, "smtp_code", None)
    if isinstance(error, smtplib.SMTPRecipientsRefused):
        codes = sorted({str(item[0]) for item in error.recipients.values() if isinstance(item, tuple)})
        code = ",".join(codes) or None
    name = type(error).__name__
    return f"{name} {code}" if code else name


def autoreply_snapshot(rule: Autoreply, attachments: list[Attachment], settings: dict[str, str]) -> dict[str, Any]:
    """Frozen copy stored in the encrypted outbox: later edits do not change a started delivery."""
    return {
        "id": rule.id,
        "name": rule.name,
        "sender_name": rule.sender_name or settings["sender_name"],
        "sender_email": settings["sender_email"],
        "reply_to": rule.reply_to,
        "subject_template": rule.subject_template,
        "html_template": rule.html_template,
        "text_template": rule.text_template,
        "attachments": [
            {"sha256": item.sha256, "filename": item.filename, "content_type": item.content_type}
            for item in attachments
        ],
    }


@dataclass(slots=True)
class Submission:
    fields: dict[str, Any]
    received_at: datetime


class MailWorker:
    HOUSEKEEPING_SECONDS = 3600

    def __init__(self, config: Config, database: Database, cipher: QueueCipher):
        self.config = config
        self.database = database
        self.cipher = cipher
        self._wake = asyncio.Event()
        self._stopping = False
        self._housekeeping_at = 0.0

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stopping = True
        self._wake.set()

    def housekeeping(self) -> None:
        self.database.purge_history(
            retention_seconds=self.config.history_retention_days * 86400,
            queue_retention_seconds=self.config.queue_retention_seconds,
        )
        self.database.purge_attachments(self.config.queue_retention_seconds + 86400)

    async def run(self) -> None:
        while not self._stopping:
            if time.monotonic() - self._housekeeping_at > self.HOUSEKEEPING_SECONDS or not self._housekeeping_at:
                self._housekeeping_at = time.monotonic()
                try:
                    await asyncio.to_thread(self.housekeeping)
                except Exception:  # noqa: BLE001 - housekeeping never stops delivery
                    pass
            try:
                item = self.database.claim_next(
                    self.config.queue_lease_seconds,
                    self.config.dedupe_retention_seconds,
                )
            except Exception:  # noqa: BLE001 - worker must survive transient database failures
                await asyncio.sleep(1)
                continue
            if item is None:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1)
                except TimeoutError:
                    pass
                continue
            try:
                await self._process(item)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - lease safely recovers unknown processing failures
                # The lease makes the item eligible again after a database error.
                await asyncio.sleep(1)

    def _plan(self, item: OutboxItem, submission: Submission) -> dict[str, Any]:
        """Freeze who receives what. History rows are idempotent per outbox item."""
        settings = self._settings(submission)
        pending = parse_recipients(settings["recipients"])
        reference = self.cipher.reference(submission.fields)
        form_key = str(submission.fields.get("formid") or submission.fields.get("formname") or "")[:300]
        common = {"submission_ref": reference, "form_key": form_key}
        subject = render_subject(settings["subject_template"], submission.fields)
        history = {
            address.casefold(): self.database.history_open(
                delivery_key=f"{item.id}:n:{address.casefold()}", kind="notification", recipient=address,
                rule_id=settings.get("route_id"), rule_name=settings.get("route_name", "По умолчанию"),
                subject=subject, **common,
            )
            for address in pending
        }
        delivery: dict[str, Any] = {"settings": settings, "pending": pending, "history": history, "autoreply": None}
        rule = self.database.matching_autoreply(submission.fields)
        if rule is None:
            return delivery
        to = submitter_email(submission.fields, rule.email_field)
        if to is None or not settings["sender_email"].strip():
            self.database.history_open(
                delivery_key=f"{item.id}:a", kind="autoreply", recipient="", rule_id=rule.id,
                rule_name=rule.name, status="skipped",
                error="no_sender" if to else "no_valid_email", **common,
            )
            return delivery
        snapshot = autoreply_snapshot(rule, self.database.get_attachments(rule.id), settings)
        snapshot["to"] = to
        snapshot["history_id"] = self.database.history_open(
            delivery_key=f"{item.id}:a", kind="autoreply", recipient=to, rule_id=rule.id, rule_name=rule.name,
            subject=render_subject(rule.subject_template, submission.fields),
            attachments=len(snapshot["attachments"]), **common,
        )
        delivery["autoreply"] = snapshot
        return delivery

    async def _process(self, item: OutboxItem) -> None:
        try:
            payload = self.cipher.decrypt_payload(item.id, item.encrypted)
            submission = Submission(
                fields=payload["fields"], received_at=datetime.fromisoformat(payload["received_at"]),
            )
        except Exception:  # noqa: BLE001 - authenticated payload is retained for key/config repair
            # Preserve ciphertext so an accidentally removed/wrong key can be fixed.
            self.database.reschedule(
                item,
                self.config.retry_max_seconds,
                self.config.dedupe_retention_seconds,
            )
            return

        try:
            delivery = payload.get("delivery")
            if delivery is None:
                delivery = self._plan(item, submission)
            history = delivery.get("history") or {}

            def checkpoint() -> None:
                encrypted = self.cipher.encrypt(item.id, submission.fields, submission.received_at, delivery)
                self.database.save_delivery_progress(item, encrypted, self.config.queue_lease_seconds)

            failed = False
            for recipient in list(delivery["pending"]):
                if self._stopping:
                    raise RuntimeError("Worker is stopping")
                checkpoint()  # Freeze settings before SMTP and renew the lease for each recipient.
                try:
                    await asyncio.to_thread(
                        self._send, submission, item.dedupe_key,
                        settings=delivery["settings"], recipient=recipient,
                    )
                except Exception as error:  # noqa: BLE001 - a failed recipient must not block the others
                    failed = True
                    self.database.history_update(
                        history.get(recipient.casefold()), status="retrying", error=error_code(error),
                    )
                    continue
                delivery["pending"].remove(recipient)
                checkpoint()  # Persist each acceptance before attempting the next recipient.
                self.database.history_update(history.get(recipient.casefold()), status="sent")
            autoreply = delivery.get("autoreply")
            if autoreply and not autoreply.get("done"):
                if self._stopping:
                    raise RuntimeError("Worker is stopping")
                checkpoint()
                try:
                    await asyncio.to_thread(self._send_autoreply, submission, item.dedupe_key, autoreply)
                except Exception as error:  # noqa: BLE001 - retried together with the item
                    failed = True
                    self.database.history_update(
                        autoreply.get("history_id"), status="retrying", error=error_code(error),
                    )
                else:
                    autoreply["done"] = True
                    checkpoint()
                    self.database.history_update(autoreply.get("history_id"), status="sent")
            if failed:
                raise RuntimeError("Some recipients require a retry")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - SMTP libraries raise several unrelated exception types
            exponent = min(max(item.attempts - 1, 0), 20)
            delay = min(self.config.retry_base_seconds * (2**exponent), self.config.retry_max_seconds)
            self.database.reschedule(item, delay, self.config.dedupe_retention_seconds)
        else:
            self.database.mark_sent(item, self.config.dedupe_retention_seconds)

    def _settings(self, submission: Submission) -> dict[str, Any]:
        settings: dict[str, Any] = self.database.get_settings()
        route = self.database.matching_route(submission.fields)
        if route is not None:
            settings.update(
                {
                    "recipients": route.recipients,
                    "subject_template": route.subject_template,
                    "email_template": route.email_template,
                    "sender_name": route.sender_name or settings["sender_name"],
                    "route_id": route.id,
                    "route_name": route.name,
                }
            )
        return settings

    def _smtp(self) -> smtplib.SMTP:
        if not self.config.smtp_host:
            raise ValueError("SMTP_HOST is not configured")
        context = ssl.create_default_context()
        if self.config.smtp_security == "ssl":
            smtp: smtplib.SMTP = smtplib.SMTP_SSL(
                self.config.smtp_host,
                self.config.smtp_port,
                timeout=self.config.smtp_timeout_seconds,
                context=context,
            )
        else:
            smtp = smtplib.SMTP(
                self.config.smtp_host,
                self.config.smtp_port,
                timeout=self.config.smtp_timeout_seconds,
            )
        return smtp

    def _open(self, smtp: smtplib.SMTP) -> None:
        if self.config.smtp_security == "starttls":
            smtp.starttls(context=ssl.create_default_context())
        if self.config.smtp_username:
            smtp.login(self.config.smtp_username, self.config.smtp_password)

    def _send(
        self, submission: Submission, dedupe_key: bytes, *,
        settings: dict[str, str] | None = None, recipient: str | None = None,
    ) -> None:
        settings = settings if settings is not None else self._settings(submission)
        recipients = [recipient] if recipient is not None else parse_recipients(settings["recipients"])
        sender_email = settings["sender_email"].strip()
        if not sender_email or sender_email.count("@") != 1 or "\n" in sender_email or "\r" in sender_email:
            raise ValueError("Sender email is not configured")
        if not self.config.smtp_host:
            raise ValueError("SMTP_HOST is not configured")

        received_at = submission.received_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        message = EmailMessage()
        message["From"] = formataddr((settings["sender_name"], sender_email))
        message["Subject"] = render_subject(settings["subject_template"], submission.fields)
        reply_to = safe_reply_to(submission.fields)
        if reply_to:
            message["Reply-To"] = reply_to
        message.set_content("Новая заявка с сайта. Откройте HTML-версию письма для просмотра полей.")
        message.add_alternative(
            render_html(settings["email_template"], submission.fields, received_at),
            subtype="html",
        )

        with self._smtp() as smtp:
            self._open(smtp)
            for address in recipients:
                # Independent SMTP envelopes and headers, stable across retries for this recipient.
                recipient_message = deepcopy(message)
                recipient_message["To"] = address
                recipient_key = hmac.new(dedupe_key, address.casefold().encode(), hashlib.sha256).hexdigest()
                recipient_message["Message-ID"] = f"<tilda-{recipient_key}@{sender_email.rsplit('@', 1)[1]}>"
                refused = smtp.send_message(recipient_message, from_addr=sender_email, to_addrs=[address])
                if refused:
                    raise smtplib.SMTPRecipientsRefused(refused)

    def build_autoreply(
        self, snapshot: dict[str, Any], fields: dict[str, Any], received_at: datetime, *,
        to: str, message_key: str, subject_prefix: str = "",
    ) -> EmailMessage:
        sender_email = str(snapshot["sender_email"]).strip()
        if not valid_email(sender_email):
            raise ValueError("Sender email is not configured")
        if not valid_email(to):
            raise ValueError("Invalid autoreply recipient")
        received = received_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        html_body = render_html(snapshot["html_template"], fields, received)
        text_template = snapshot.get("text_template") or ""
        text_body = render_text(text_template, fields, received) if text_template.strip() else html_to_text(html_body)
        message = EmailMessage()
        message["From"] = formataddr((snapshot["sender_name"], sender_email))
        message["To"] = to
        message["Subject"] = (subject_prefix + render_subject(snapshot["subject_template"], fields))[:250]
        reply_to = str(snapshot.get("reply_to") or "").strip()
        if reply_to and valid_email(reply_to):
            message["Reply-To"] = reply_to
        # RFC 3834: other robots must not answer an automatic reply.
        message["Auto-Submitted"] = "auto-replied"
        message["X-Auto-Response-Suppress"] = "All"
        message["Message-ID"] = f"<qube-autoreply-{message_key}@{sender_email.rsplit('@', 1)[1]}>"
        message.set_content(text_body or " ")
        message.add_alternative(html_body, subtype="html")
        for attachment in snapshot.get("attachments") or []:
            try:
                data = self.database.attachment_path(attachment["sha256"]).read_bytes()
            except (OSError, ValueError):
                raise RuntimeError("Attachment file is missing") from None
            maintype, _, subtype = str(attachment["content_type"]).partition("/")
            message.add_attachment(
                data, maintype=maintype or "application", subtype=subtype or "octet-stream",
                filename=attachment["filename"],
            )
        return message

    def _deliver(self, message: EmailMessage, to: str) -> None:
        sender_email = parseaddr(str(message["From"]))[1]
        with self._smtp() as smtp:
            self._open(smtp)
            refused = smtp.send_message(message, from_addr=sender_email, to_addrs=[to])
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)

    def _send_autoreply(self, submission: Submission, dedupe_key: bytes, snapshot: dict[str, Any]) -> None:
        to = snapshot["to"]
        key = hmac.new(dedupe_key, f"autoreply:{to.casefold()}".encode(), hashlib.sha256).hexdigest()
        message = self.build_autoreply(
            snapshot, submission.fields, submission.received_at, to=to, message_key=key,
        )
        self._deliver(message, to)

    def send_test(self, rule: Autoreply, to: str, fields: dict[str, Any], *, source: str = "admin") -> tuple[bool, str]:
        """Immediate test letter to any address; the result is written to the history."""
        settings = self.database.get_settings()
        snapshot = autoreply_snapshot(rule, self.database.get_attachments(rule.id), settings)
        history_id = self.database.history_open(
            delivery_key=None, kind="test", recipient=to, rule_id=rule.id, rule_name=rule.name,
            subject="[Тест] " + render_subject(rule.subject_template, fields),
            attachments=len(snapshot["attachments"]), form_key=source[:300],
        )
        try:
            message = self.build_autoreply(
                snapshot, fields, datetime.now(UTC), to=to, message_key=f"test-{uuid.uuid4().hex}",
                subject_prefix="[Тест] ",
            )
            self._deliver(message, to)
        except Exception as error:  # noqa: BLE001 - reported to the admin, never raised
            code = error_code(error)
            self.database.history_update(history_id, status="failed", error=code)
            return False, code
        self.database.history_update(history_id, status="sent")
        return True, ""
