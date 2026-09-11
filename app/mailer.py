from __future__ import annotations

import asyncio
import hashlib
import hmac
import smtplib
import ssl
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import EmailMessage
from email.utils import formataddr
from typing import Any

from app.config import Config
from app.database import Database, OutboxItem
from app.queue_crypto import QueueCipher
from app.templating import render_html, render_subject


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
        if key.lower() in {"email", "e-mail", "mail"}:
            candidate = str(value).strip()
            if "\r" not in candidate and "\n" not in candidate and candidate.count("@") == 1:
                return candidate
    return None


@dataclass(slots=True)
class Submission:
    fields: dict[str, Any]
    received_at: datetime


class MailWorker:
    def __init__(self, config: Config, database: Database, cipher: QueueCipher):
        self.config = config
        self.database = database
        self.cipher = cipher
        self._wake = asyncio.Event()
        self._stopping = False

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stopping = True
        self._wake.set()

    async def run(self) -> None:
        while not self._stopping:
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
                settings = self._settings(submission)
                delivery = {"settings": settings, "pending": parse_recipients(settings["recipients"])}

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
                except Exception:  # noqa: BLE001 - a failed recipient must not block the others
                    failed = True
                    continue
                delivery["pending"].remove(recipient)
                checkpoint()  # Persist each acceptance before attempting the next recipient.
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

    def _settings(self, submission: Submission) -> dict[str, str]:
        settings = self.database.get_settings()
        route = self.database.matching_route(submission.fields)
        if route is not None:
            settings.update(
                {
                    "recipients": route.recipients,
                    "subject_template": route.subject_template,
                    "email_template": route.email_template,
                    "sender_name": route.sender_name or settings["sender_name"],
                }
            )
        return settings

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
        with smtp:
            if self.config.smtp_security == "starttls":
                smtp.starttls(context=context)
            if self.config.smtp_username:
                smtp.login(self.config.smtp_username, self.config.smtp_password)
            for address in recipients:
                # Independent SMTP envelopes and headers, stable across retries for this recipient.
                recipient_message = deepcopy(message)
                recipient_message["To"] = address
                recipient_key = hmac.new(dedupe_key, address.casefold().encode(), hashlib.sha256).hexdigest()
                recipient_message["Message-ID"] = f"<tilda-{recipient_key}@{sender_email.rsplit('@', 1)[1]}>"
                refused = smtp.send_message(recipient_message, from_addr=sender_email, to_addrs=[address])
                if refused:
                    raise smtplib.SMTPRecipientsRefused(refused)
