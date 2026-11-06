"""Admin pages for autoreplies («отбивки»), their attachments, previews, tests and the send history."""

from __future__ import annotations

import asyncio
import mimetypes
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import PurePath
from typing import Any
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import ANY_FORM, DEFAULT_AUTOREPLY_TEMPLATE, HISTORY_KINDS, HISTORY_STATUSES, Autoreply, Database
from app.mailer import MailWorker, valid_email
from app.templating import render_html

MAX_ATTACHMENT_BYTES = 10 * 1024 * 1024
MAX_ATTACHMENTS_TOTAL = 15 * 1024 * 1024
MAX_ATTACHMENTS = 10
ALLOWED_EXTENSIONS = {
    ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".doc", ".docx", ".xls", ".xlsx", ".ppt",
    ".pptx", ".odt", ".ods", ".rtf", ".txt", ".csv", ".ics", ".zip",
}
PAGE_SIZE = 50
STATUS_LABELS = {
    "queued": "В очереди", "retrying": "Повтор", "sent": "Отправлено", "failed": "Ошибка",
    "skipped": "Пропущено", "expired": "Истёк срок",
}
KIND_LABELS = {"notification": "Организаторам", "autoreply": "Отбивка", "test": "Тест"}
DEFAULT_SUBJECT = "Спасибо! Мы получили вашу заявку"


def safe_filename(raw: str) -> str:
    name = PurePath(raw.replace("\\", "/")).name
    name = "".join(char for char in name if char >= " " and char != "\x7f" and char not in '"<>|:*?')
    name = name.strip(" .")
    if len(name) > 150:
        stem, dot, extension = name.rpartition(".")
        name = (stem[: 150 - len(extension) - 1] + dot + extension) if dot else name[:150]
    return name


def parse_test_values(raw: str) -> dict[str, str]:
    """«Name: Иван» per line → sample form fields for a test letter (max 100 lines)."""
    values: dict[str, str] = {}
    for line in raw.splitlines()[:100]:
        key, separator, value = line.partition(":")
        key = key.strip()
        if separator and key:
            values[key[:200]] = value.strip()[:2000]
    return values


def sample_fields(database: Database, rule: Autoreply | None) -> dict[str, str]:
    names = database.form_field_names(rule.match_field, rule.match_value) if rule else []
    names = names or ["Name", "Email", "Phone"]
    return {name: f"[{name}]" for name in names}


def register_autoreply_views(
    app: FastAPI, *, database: Database, worker: MailWorker, templates: Jinja2Templates,
    require_admin: Callable[..., str], csrf_token: Callable[[], str],
    check_csrf: Callable[[str], None], validate_sender_name: Callable[[str], str],
) -> None:
    def require_autoreply(autoreply_id: int) -> Autoreply:
        rule = database.get_autoreply(autoreply_id)
        if rule is None:
            raise HTTPException(status_code=404, detail="Отбивка не найдена")
        return rule

    @app.get("/admin/autoreplies", response_class=HTMLResponse)
    async def autoreplies_page(
        request: Request, _: str = Depends(require_admin), page: int = Query(1, ge=1, le=10_000),
    ):
        total = database.count_autoreplies()
        rules = database.get_autoreplies(limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE)
        return templates.TemplateResponse(request=request, name="autoreplies.html", context={
            "rules": rules, "attachments": database.attachment_counts(),
            "sent": database.autoreply_sent_counts(), "page": page,
            "pages": max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE), "total": total,
            "saved": request.query_params.get("saved") == "1", "any_form": ANY_FORM,
        })

    def page_context(request: Request, rule: Autoreply | None, **extra: Any) -> dict[str, Any]:
        settings = database.get_settings()
        if rule is None:
            query = request.query_params
            prefill = {
                "name": query.get("name", "")[:100], "enabled": True,
                "match_field": query.get("match_field", "formid")[:200] or "formid",
                "match_value": query.get("match_value", "")[:500], "email_field": "",
                "sender_name": "", "reply_to": "", "subject_template": DEFAULT_SUBJECT,
                "html_template": DEFAULT_AUTOREPLY_TEMPLATE, "text_template": "", "any_form": False, "id": None,
            }
        else:
            prefill = {
                "id": rule.id, "name": rule.name, "enabled": rule.enabled,
                "match_field": "" if rule.any_form else rule.match_field,
                "match_value": "" if rule.any_form else rule.match_value, "email_field": rule.email_field,
                "sender_name": rule.sender_name, "reply_to": rule.reply_to,
                "subject_template": rule.subject_template, "html_template": rule.html_template,
                "text_template": rule.text_template, "any_form": rule.any_form,
            }
        known = database.known_form_values()
        sample = sample_fields(database, rule)
        return {
            "rule": prefill, "is_new": rule is None, "settings": settings, "csrf_token": csrf_token(),
            "attachments": database.get_attachments(rule.id) if rule else [],
            "history": database.history_page(rule_id=rule.id, limit=20).rows if rule else [],
            "known": known, "sample_text": "\n".join(f"{key}: " for key in sample),
            "field_names": list(sample), "max_attachment_mb": MAX_ATTACHMENT_BYTES // 1024 // 1024,
            "max_total_mb": MAX_ATTACHMENTS_TOTAL // 1024 // 1024, "max_attachments": MAX_ATTACHMENTS,
            "allowed": ", ".join(sorted(ALLOWED_EXTENSIONS)), "status_labels": STATUS_LABELS,
            "kind_labels": KIND_LABELS, **extra,
        }

    @app.get("/admin/autoreplies/new", response_class=HTMLResponse)
    async def new_autoreply_page(request: Request, _: str = Depends(require_admin)):
        return templates.TemplateResponse(request=request, name="autoreply.html", context=page_context(request, None))

    @app.get("/admin/autoreplies/{autoreply_id}", response_class=HTMLResponse)
    async def autoreply_page(autoreply_id: int, request: Request, _: str = Depends(require_admin)):
        query = request.query_params
        return templates.TemplateResponse(request=request, name="autoreply.html", context=page_context(
            request, require_autoreply(autoreply_id), saved=query.get("saved") == "1",
            tested=query.get("tested", "")[:320], test_error=query.get("test_error", "")[:120],
        ))

    def validate(
        *, name: str, match_mode: str, match_field: str, match_value: str, email_field: str,
        sender_name: str, reply_to: str, subject_template: str, html_template: str, text_template: str,
        enabled: str | None,
    ) -> dict[str, Any]:
        values: dict[str, Any] = {
            "name": name.strip(), "match_field": match_field.strip(), "match_value": match_value.strip(),
            "email_field": email_field.strip(), "sender_name": validate_sender_name(sender_name),
            "reply_to": reply_to.strip(), "subject_template": subject_template.strip(),
            "html_template": html_template.strip(), "text_template": text_template.strip(),
            "enabled": enabled == "1",
        }
        if match_mode == "any":
            values["match_field"] = values["match_value"] = ANY_FORM
        elif not values["match_field"] or not values["match_value"] or values["match_field"] == ANY_FORM:
            raise HTTPException(status_code=422, detail="Укажите поле формы и его значение")
        if not values["name"] or not values["subject_template"] or not values["html_template"]:
            raise HTTPException(status_code=422, detail="Название, тема и HTML-вёрстка обязательны")
        limits = {"name": 100, "match_field": 200, "match_value": 500, "email_field": 200, "reply_to": 254,
                  "subject_template": 1_000, "html_template": 200_000, "text_template": 100_000}
        for key, limit in limits.items():
            if len(values[key]) > limit:
                raise HTTPException(status_code=422, detail=f"Поле {key} слишком длинное")
        if values["reply_to"] and not valid_email(values["reply_to"]):
            raise HTTPException(status_code=422, detail="Некорректный адрес для ответа (Reply-To)")
        if any(char in values["email_field"] for char in "\r\n"):
            raise HTTPException(status_code=422, detail="Некорректное имя поля с email")
        return values

    async def read_uploads(files: list[UploadFile], existing_bytes: int, existing_count: int) -> list[tuple[str, str, bytes]]:
        uploads = []
        total = existing_bytes
        for upload in files:
            if not upload.filename:
                continue
            filename = safe_filename(upload.filename)
            extension = PurePath(filename).suffix.lower()
            if not filename or extension not in ALLOWED_EXTENSIONS:
                raise HTTPException(status_code=422, detail=f"Тип файла не разрешён: {filename or 'без имени'}")
            data = await upload.read(MAX_ATTACHMENT_BYTES + 1)
            if len(data) > MAX_ATTACHMENT_BYTES:
                raise HTTPException(status_code=422, detail=f"Файл {filename} больше {MAX_ATTACHMENT_BYTES // 1048576} МБ")
            if not data:
                raise HTTPException(status_code=422, detail=f"Файл {filename} пустой")
            total += len(data)
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            uploads.append((filename, content_type, data))
        if existing_count + len(uploads) > MAX_ATTACHMENTS:
            raise HTTPException(status_code=422, detail=f"Не больше {MAX_ATTACHMENTS} вложений")
        if total > MAX_ATTACHMENTS_TOTAL:
            raise HTTPException(status_code=422, detail=f"Вложения вместе больше {MAX_ATTACHMENTS_TOTAL // 1048576} МБ")
        return uploads

    async def save(
        autoreply_id: int | None, *, csrf: str, values: dict[str, Any], files: list[UploadFile],
        remove_attachment: list[int],
    ) -> RedirectResponse:
        check_csrf(csrf)
        current = database.get_attachments(autoreply_id) if autoreply_id else []
        kept = [item for item in current if item.id not in set(remove_attachment)]
        uploads = await read_uploads(files, sum(item.size for item in kept), len(kept))
        try:
            saved_id = database.save_autoreply(autoreply_id, values)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        database.remove_attachments(saved_id, list(remove_attachment))
        for filename, content_type, data in uploads:
            await asyncio.to_thread(database.add_attachment, saved_id, filename, content_type, data)
        return RedirectResponse(f"/admin/autoreplies/{saved_id}?saved=1", status_code=303)

    @app.post("/admin/autoreplies")
    async def create_autoreply(
        _: str = Depends(require_admin), csrf: str = Form(...), name: str = Form(...),
        match_mode: str = Form("form"), match_field: str = Form(""), match_value: str = Form(""),
        email_field: str = Form(""), sender_name: str = Form(""), reply_to: str = Form(""),
        subject_template: str = Form(...), html_template: str = Form(...), text_template: str = Form(""),
        enabled: str | None = Form(None), files: list[UploadFile] = File(default=[]),  # noqa: B008
    ):
        check_csrf(csrf)
        values = validate(
            name=name, match_mode=match_mode, match_field=match_field, match_value=match_value,
            email_field=email_field, sender_name=sender_name, reply_to=reply_to,
            subject_template=subject_template, html_template=html_template, text_template=text_template,
            enabled=enabled,
        )
        return await save(None, csrf=csrf, values=values, files=files, remove_attachment=[])

    @app.post("/admin/autoreplies/{autoreply_id}")
    async def update_autoreply(
        autoreply_id: int, _: str = Depends(require_admin), csrf: str = Form(...),
        operation: str = Form("save"), name: str = Form(""),
        match_mode: str = Form("form"), match_field: str = Form(""), match_value: str = Form(""),
        email_field: str = Form(""), sender_name: str = Form(""), reply_to: str = Form(""),
        subject_template: str = Form(""), html_template: str = Form(""), text_template: str = Form(""),
        enabled: str | None = Form(None), files: list[UploadFile] = File(default=[]),  # noqa: B008
        remove_attachment: list[int] = Form(default=[]),  # noqa: B008
    ):
        check_csrf(csrf)
        require_autoreply(autoreply_id)
        if operation == "delete":
            database.delete_autoreply(autoreply_id)
            return RedirectResponse("/admin/autoreplies?saved=1", status_code=303)
        values = validate(
            name=name, match_mode=match_mode, match_field=match_field, match_value=match_value,
            email_field=email_field, sender_name=sender_name, reply_to=reply_to,
            subject_template=subject_template, html_template=html_template, text_template=text_template,
            enabled=enabled,
        )
        return await save(autoreply_id, csrf=csrf, values=values, files=files, remove_attachment=remove_attachment)

    @app.get("/admin/autoreplies/{autoreply_id}/preview", response_class=HTMLResponse)
    async def autoreply_preview(autoreply_id: int, _: str = Depends(require_admin)):
        rule = require_autoreply(autoreply_id)
        fields = sample_fields(database, rule)
        received = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        body = render_html(rule.html_template, fields, received, show_missing=True)
        response = HTMLResponse(body)
        # Shown inside the admin page only; scripts and forms of the layout never run.
        response.headers["Content-Security-Policy"] = (
            "sandbox; default-src 'none'; img-src https: data:; style-src 'unsafe-inline'; "
            "font-src https: data:; frame-ancestors 'self'"
        )
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        return response

    @app.post("/admin/autoreplies/{autoreply_id}/test")
    async def autoreply_test(
        autoreply_id: int, _: str = Depends(require_admin), csrf: str = Form(...),
        to: str = Form(...), values: str = Form(""),
    ):
        check_csrf(csrf)
        rule = require_autoreply(autoreply_id)
        to = to.strip()
        if not valid_email(to):
            raise HTTPException(status_code=422, detail="Укажите корректный email для теста")
        fields = parse_test_values(values)
        ok, code = await asyncio.to_thread(worker.send_test, rule, to, fields, source="admin")
        query = urlencode({"tested": to} if ok else {"test_error": code or "error"})
        return RedirectResponse(f"/admin/autoreplies/{autoreply_id}?{query}#test", status_code=303)

    @app.get("/admin/history", response_class=HTMLResponse)
    async def history_page(
        request: Request, _: str = Depends(require_admin),
        kind: str = Query("", max_length=20), status: str = Query("", max_length=20),
        rule: int | None = Query(None, ge=1), recipient: str = Query("", max_length=320),
        start_date: str = Query("", max_length=10), end_date: str = Query("", max_length=10),
        page: int = Query(1, ge=1, le=100_000), limit: int = Query(PAGE_SIZE, ge=10, le=200),
    ):
        if kind and kind not in HISTORY_KINDS or status and status not in HISTORY_STATUSES:
            raise HTTPException(status_code=422, detail="Неизвестный фильтр")
        try:
            start = datetime.combine(date.fromisoformat(start_date), datetime.min.time(), UTC) if start_date else None
            end = (datetime.combine(date.fromisoformat(end_date), datetime.min.time(), UTC) + timedelta(days=1)
                   if end_date else None)
        except ValueError:
            raise HTTPException(status_code=422, detail="Некорректная дата") from None
        result = database.history_page(
            kind=kind, status=status, rule_id=rule, recipient=recipient.strip(),
            start=start.timestamp() if start else None, end=end.timestamp() if end else None,
            limit=limit, offset=(page - 1) * limit,
        )
        pages = max(1, (result.total + limit - 1) // limit)

        def page_url(number: int) -> str:
            return str(request.url.include_query_params(page=number))

        return templates.TemplateResponse(request=request, name="history.html", context={
            "rows": result.rows, "total": result.total, "page": page, "pages": pages, "limit": limit,
            "kind": kind, "status": status, "rule": rule, "recipient": recipient,
            "start_date": start_date, "end_date": end_date, "rules": database.get_autoreplies(),
            "status_labels": STATUS_LABELS, "kind_labels": KIND_LABELS,
            "previous_url": page_url(page - 1) if page > 1 else "",
            "next_url": page_url(page + 1) if page < pages else "",
            "first_url": page_url(1), "last_url": page_url(pages),
        })

