from __future__ import annotations

import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass(frozen=True, slots=True)
class EncryptedPayload:
    key_version: int
    nonce: bytes
    ciphertext: bytes


class QueueCipher:
    def __init__(self, keys: dict[int, bytes], active_version: int, dedupe_key: bytes):
        self._keys = keys
        self.active_version = active_version
        self._dedupe_key = dedupe_key

    @staticmethod
    def _aad(event_id: str, key_version: int) -> bytes:
        return f"tilda-outbox:v1:{event_id}:{key_version}".encode("ascii")

    def encrypt(
        self, event_id: str, fields: dict[str, Any], received_at: datetime,
        delivery: dict[str, Any] | None = None,
    ) -> EncryptedPayload:
        payload = json.dumps(
            {"fields": fields, "received_at": received_at.isoformat(), "delivery": delivery},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        nonce = os.urandom(12)
        version = self.active_version
        ciphertext = AESGCM(self._keys[version]).encrypt(nonce, payload, self._aad(event_id, version))
        return EncryptedPayload(version, nonce, ciphertext)

    def decrypt(self, event_id: str, encrypted: EncryptedPayload) -> tuple[dict[str, Any], datetime]:
        payload = self.decrypt_payload(event_id, encrypted)
        return payload["fields"], datetime.fromisoformat(payload["received_at"])

    def decrypt_payload(self, event_id: str, encrypted: EncryptedPayload) -> dict[str, Any]:
        key = self._keys.get(encrypted.key_version)
        if key is None:
            raise RuntimeError(f"Queue key version {encrypted.key_version} is unavailable")
        plaintext = AESGCM(key).decrypt(
            encrypted.nonce,
            encrypted.ciphertext,
            self._aad(event_id, encrypted.key_version),
        )
        return json.loads(plaintext.decode("utf-8"))

    def dedupe(self, fields: dict[str, Any], event_id: str) -> bytes:
        tranid = str(fields.get("tranid", "")).strip()
        formid = str(fields.get("formid", "")).strip()
        if tranid:
            source = f"tilda:{formid}:{tranid}".encode()
        else:
            # Without Tilda's stable lead id, treating identical form values as a
            # duplicate could silently drop a legitimate second submission.
            source = f"event:{event_id}".encode("ascii")
        return hmac.new(self._dedupe_key, source, hashlib.sha256).digest()
