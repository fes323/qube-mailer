from __future__ import annotations

import base64
import binascii
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Config:
    database_path: Path
    webhook_api_key: str
    webhook_api_key_header: str
    admin_username: str
    admin_password: str
    csrf_secret: str
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_security: str
    smtp_timeout_seconds: float
    queue_size: int
    max_body_bytes: int
    queue_keys: dict[int, bytes]
    active_queue_key_version: int
    queue_dedupe_key: bytes
    queue_retention_seconds: int
    dedupe_retention_seconds: int
    queue_lease_seconds: int
    retry_base_seconds: int
    retry_max_seconds: int
    smtp_bz_api_key: str = ""

    @classmethod
    def from_env(cls) -> Config:
        security = os.getenv("SMTP_SECURITY", "starttls").strip().lower()
        if security not in {"starttls", "ssl", "none"}:
            raise ValueError("SMTP_SECURITY must be one of: starttls, ssl, none")
        keyring_path = Path(os.getenv("QUEUE_KEYRING_FILE", "/run/secrets/queue_keyring"))
        queue_keys, active_version, dedupe_key = _load_keyring(keyring_path)
        return cls(
            database_path=Path(os.getenv("DATABASE_PATH", "/data/app.db")),
            webhook_api_key=os.getenv("WEBHOOK_API_KEY", ""),
            webhook_api_key_header=os.getenv("WEBHOOK_API_KEY_HEADER", "X-Webhook-Key"),
            admin_username=os.getenv("ADMIN_USERNAME", "admin"),
            admin_password=os.getenv("ADMIN_PASSWORD", ""),
            csrf_secret=os.getenv("CSRF_SECRET", ""),
            smtp_host=os.getenv("SMTP_HOST", ""),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_username=os.getenv("SMTP_USERNAME", ""),
            smtp_password=os.getenv("SMTP_PASSWORD", ""),
            smtp_security=security,
            smtp_timeout_seconds=float(os.getenv("SMTP_TIMEOUT_SECONDS", "15")),
            queue_size=int(os.getenv("QUEUE_SIZE", "100")),
            max_body_bytes=int(os.getenv("MAX_BODY_BYTES", "1048576")),
            queue_keys=queue_keys,
            active_queue_key_version=active_version,
            queue_dedupe_key=dedupe_key,
            queue_retention_seconds=int(os.getenv("QUEUE_RETENTION_SECONDS", "604800")),
            dedupe_retention_seconds=int(os.getenv("DEDUPE_RETENTION_SECONDS", "604800")),
            queue_lease_seconds=int(os.getenv("QUEUE_LEASE_SECONDS", "300")),
            retry_base_seconds=int(os.getenv("RETRY_BASE_SECONDS", "5")),
            retry_max_seconds=int(os.getenv("RETRY_MAX_SECONDS", "3600")),
            smtp_bz_api_key=os.getenv("SMTP_BZ_API_KEY", "").strip(),
        )

    def validate(self) -> None:
        missing = [
            name
            for name, value in (
                ("WEBHOOK_API_KEY", self.webhook_api_key),
                ("ADMIN_PASSWORD", self.admin_password),
                ("CSRF_SECRET", self.csrf_secret),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(f"Required environment variables are missing: {', '.join(missing)}")
        weak = [
            name
            for name, value in (
                ("WEBHOOK_API_KEY", self.webhook_api_key),
                ("ADMIN_PASSWORD", self.admin_password),
                ("CSRF_SECRET", self.csrf_secret),
            )
            if len(value) < 32 or value.lower().startswith("generate-")
        ]
        if weak:
            raise RuntimeError(f"Secrets must be at least 32 characters long: {', '.join(weak)}")
        if len({self.webhook_api_key, self.admin_password, self.csrf_secret}) != 3:
            raise RuntimeError("WEBHOOK_API_KEY, ADMIN_PASSWORD and CSRF_SECRET must be different")
        if not re.fullmatch(r"[A-Za-z0-9-]+", self.webhook_api_key_header):
            raise ValueError("WEBHOOK_API_KEY_HEADER must be a valid HTTP header name")
        if self.active_queue_key_version not in self.queue_keys:
            raise RuntimeError("The active queue key is missing from the keyring")
        if any(len(key) != 32 for key in self.queue_keys.values()) or len(self.queue_dedupe_key) != 32:
            raise RuntimeError("Queue encryption and dedupe keys must be exactly 32 bytes")
        if any(version < 1 for version in self.queue_keys):
            raise RuntimeError("Queue key versions must be positive integers")
        if self.queue_dedupe_key in self.queue_keys.values():
            raise RuntimeError("Queue dedupe key must be different from every encryption key")
        positive = (
            self.queue_size,
            self.max_body_bytes,
            self.queue_retention_seconds,
            self.dedupe_retention_seconds,
            self.queue_lease_seconds,
            self.retry_base_seconds,
            self.retry_max_seconds,
        )
        if any(value < 1 for value in positive):
            raise ValueError("Queue limits and timeouts must be positive")
        if self.retry_base_seconds > self.retry_max_seconds:
            raise ValueError("RETRY_BASE_SECONDS cannot exceed RETRY_MAX_SECONDS")


def _decode_key(value: str, name: str) -> bytes:
    try:
        decoded = base64.b64decode(value.strip(), validate=True)
    except (ValueError, binascii.Error) as error:
        raise RuntimeError(f"{name} must be valid base64") from error
    if len(decoded) != 32:
        raise RuntimeError(f"{name} must decode to exactly 32 bytes")
    return decoded


def _load_keyring(path: Path) -> tuple[dict[int, bytes], int, bytes]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        active = int(raw["active"])
        keys = {int(version): _decode_key(value, f"keys.{version}") for version, value in raw["keys"].items()}
        dedupe_key = _decode_key(raw["dedupe_key"], "dedupe_key")
    except FileNotFoundError as error:
        raise RuntimeError(f"Queue keyring file not found: {path}") from error
    except PermissionError as error:
        raise RuntimeError(
            f"Queue keyring is not readable: {path}. Run docker compose up -d --build "
            "to prepare the app-owned secret, or grant the local process read access."
        ) from error
    except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Invalid queue keyring file: {path}") from error
    if active not in keys:
        raise RuntimeError("Active queue key version is not present in keyring")
    return keys, active, dedupe_key
