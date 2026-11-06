"""JSON API for QUBE CMS: forms catalogue, autoreplies, test letters and the send history.

Authentication: ``Authorization: Bearer <INTEGRATION_API_KEY>``. Without the key the API answers 404.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request

from app.config import Config
from app.database import CMS_FORMS_MAX_ENTRIES, HISTORY_KINDS, HISTORY_STATUSES, Database
from app.mailer import MailWorker, valid_email
from app.queue_crypto import QueueCipher

MAX_JSON_BYTES = 1024 * 1024
FORM_LIMITS = {"formid": 255, "formname": 300, "form_route": 100, "kind_label": 100, "page_url": 500, "admin_url": 500}


def _clean(value: Any, limit: int) -> str:
    text = "".join(char for char in str(value or "").strip() if char >= " " and char != "\x7f")
    return text[:limit]


async def _json(request: Request) -> Any:
    if not request.headers.get("content-type", "").lower().startswith("application/json"):
        raise HTTPException(status_code=415, detail="Expected application/json")
    body = await request.body()
    if len(body) > MAX_JSON_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")
    try:
        return json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Invalid JSON") from None


def register_api(app: FastAPI, *, config: Config, database: Database, worker: MailWorker, cipher: QueueCipher) -> None:
    def require_api(request: Request) -> None:
        if not config.integration_api_key:
            raise HTTPException(status_code=404, detail="Not found")
        scheme, _, supplied = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(
            supplied.strip().encode(), config.integration_api_key.encode()
        ):
            database.increment("unauthorized")
            raise HTTPException(status_code=401, detail="Invalid API key")

    def resolve(form: dict[str, str]) -> dict[str, Any]:
        fields = {key: form[key] for key in ("formid", "formname", "form_route") if form.get(key)}
        route = database.matching_route(fields)
        rule = database.matching_autoreply(fields)
        return {
            "formid": form["formid"],
            "route": {"id": route.id, "name": route.name, "admin_path": f"/admin/routes/{route.id}"} if route else None,
            "autoreply": {
                "id": rule.id, "name": rule.name, "any_form": rule.any_form,
                "admin_path": f"/admin/autoreplies/{rule.id}",
            } if rule else None,
        }

    @app.get("/api/v1/status", dependencies=[Depends(require_api)])
    async def api_status() -> dict[str, Any]:
        settings = database.get_settings()
        queue = database.queue_stats()
        return {
            "ok": True,
            "api_version": 1,
            "smtp_configured": bool(config.smtp_host and config.smtp_username),
            "sender_configured": bool(settings["sender_email"]),
            "queue": {"pending": queue.pending, "retrying": queue.retrying},
            "history_retention_days": config.history_retention_days,
        }

    @app.put("/api/v1/forms", dependencies=[Depends(require_api)])
    async def api_sync_forms(request: Request) -> dict[str, Any]:
        payload = await _json(request)
        items = payload.get("forms") if isinstance(payload, dict) else None
        if not isinstance(items, list) or len(items) > CMS_FORMS_MAX_ENTRIES:
            raise HTTPException(status_code=422, detail=f"forms must be a list of up to {CMS_FORMS_MAX_ENTRIES}")
        forms: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                raise HTTPException(status_code=422, detail="Each form must be an object")
            form = {key: _clean(item.get(key), limit) for key, limit in FORM_LIMITS.items()}
            if not form["formid"] or form["formid"] in seen:
                continue
            seen.add(form["formid"])
            forms.append(form)
        database.replace_cms_forms(forms)
        return {"forms": [resolve(form) for form in forms], "synced": len(forms)}

    @app.get("/api/v1/autoreplies", dependencies=[Depends(require_api)])
    async def api_autoreplies() -> dict[str, Any]:
        attachments = database.attachment_counts()
        sent = database.autoreply_sent_counts()
        return {"autoreplies": [
            {
                "id": rule.id, "name": rule.name, "enabled": rule.enabled, "any_form": rule.any_form,
                "match_field": "" if rule.any_form else rule.match_field,
                "match_value": "" if rule.any_form else rule.match_value,
                "attachments": attachments.get(rule.id, 0), "sent_30_days": sent.get(rule.id, 0),
                "admin_path": f"/admin/autoreplies/{rule.id}",
            }
            for rule in database.get_autoreplies()
        ]}

    @app.post("/api/v1/autoreplies/{autoreply_id}/test", dependencies=[Depends(require_api)])
    async def api_test(autoreply_id: int, request: Request) -> dict[str, Any]:
        payload = await _json(request)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Expected an object")
        rule = database.get_autoreply(autoreply_id)
        if rule is None:
            raise HTTPException(status_code=404, detail="Autoreply not found")
        to = _clean(payload.get("email"), 254)
        if not valid_email(to):
            raise HTTPException(status_code=422, detail="Invalid email")
        raw_fields = payload.get("fields") or {}
        if not isinstance(raw_fields, dict) or len(raw_fields) > 100:
            raise HTTPException(status_code=422, detail="fields must be an object with up to 100 keys")
        fields = {_clean(key, 200): _clean(value, 2000) for key, value in raw_fields.items() if _clean(key, 200)}
        source = _clean(payload.get("requested_by"), 200) or "cms"
        ok, code = await asyncio.to_thread(worker.send_test, rule, to, fields, source=f"cms:{source}")
        return {"ok": ok, "error": code}

    @app.get("/api/v1/history", dependencies=[Depends(require_api)])
    async def api_history(
        submission: str = Query("", max_length=255), email: str = Query("", max_length=320),
        kind: str = Query("", max_length=20), status: str = Query("", max_length=20),
        autoreply: int | None = Query(None, ge=1),
        limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0, le=1_000_000),
    ) -> dict[str, Any]:
        if kind and kind not in HISTORY_KINDS or status and status not in HISTORY_STATUSES:
            raise HTTPException(status_code=422, detail="Unknown filter")
        reference = cipher.reference_for(submission.strip()) if submission.strip() else ""
        page = database.history_page(
            kind=kind, status=status, rule_id=autoreply, recipient=email.strip(),
            submission_ref=reference, limit=limit, offset=offset,
        )
        rows = [
            {key: row[key] for key in ("id", "kind", "rule_id", "rule_name", "recipient", "subject", "status",
                                       "attempts", "error", "attachments", "form_key", "created", "updated")}
            for row in page.rows
        ]
        return {"total": page.total, "rows": rows}

