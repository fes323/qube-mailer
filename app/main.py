from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

from app.config import Config
from app.database import Database
from app.mailer import MailWorker, parse_recipients
from app.queue_crypto import QueueCipher
from app.smtp_bz import JournalError, fetch_messages, journal_table

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")
basic_auth = HTTPBasic(auto_error=False)


def _secure_equal(left: str, right: str) -> bool:
    return secrets.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


def create_app(config: Config | None = None) -> FastAPI:
    config = config or Config.from_env()
    config.validate()
    if os.name == "posix":
        os.umask(0o077)
    database = Database(config.database_path)
    database.initialize()
    missing_queue_keys = database.pending_key_versions() - set(config.queue_keys)
    if missing_queue_keys:
        versions = ", ".join(str(version) for version in sorted(missing_queue_keys))
        raise RuntimeError(f"Queue keyring is missing versions required by pending records: {versions}")
    cipher = QueueCipher(config.queue_keys, config.active_queue_key_version, config.queue_dedupe_key)
    worker = MailWorker(config, database, cipher)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker_task = asyncio.create_task(worker.run(), name="mail-worker")
        app.state.worker_task = worker_task
        yield
        worker.stop()
        try:
            await asyncio.wait_for(worker_task, timeout=config.smtp_timeout_seconds + 5)
        except TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

    app = FastAPI(
        title="Tilda Webhook Mailer",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.config = config
    app.state.database = database
    app.state.mail_worker = worker
    app.state.queue_cipher = cipher

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive, nosnippet, noimageindex"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
            "base-uri 'none'; frame-ancestors 'none'"
        )
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        return response

    def require_admin(
        credentials: HTTPBasicCredentials | None = Depends(basic_auth),  # noqa: B008
    ) -> str:
        valid = bool(
            credentials
            and _secure_equal(credentials.username, config.admin_username)
            and _secure_equal(credentials.password, config.admin_password)
        )
        if not valid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
                headers={"WWW-Authenticate": 'Basic realm="Webhook admin", charset="UTF-8"'},
            )
        return config.admin_username

    def csrf_token() -> str:
        return hmac.new(config.csrf_secret.encode(), b"admin-settings", hashlib.sha256).hexdigest()

    @app.get("/health/live", response_class=PlainTextResponse)
    async def live() -> str:
        return "ok"

    @app.get("/health/ready", response_class=PlainTextResponse)
    async def ready() -> str:
        database.get_settings()
        if app.state.worker_task.done():
            raise HTTPException(status_code=503, detail="Mail worker is not running")
        return "ok"

    @app.get("/robots.txt", response_class=PlainTextResponse, include_in_schema=False)
    async def robots() -> str:
        return "User-agent: *\nDisallow: /\n"

    @app.post("/webhook/tilda", response_class=PlainTextResponse)
    async def tilda_webhook(request: Request):
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > config.max_body_bytes:
                    raise HTTPException(status_code=413, detail="Payload too large")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid Content-Length")

        supplied_key = request.headers.get(config.webhook_api_key_header, "")
        if not _secure_equal(supplied_key, config.webhook_api_key):
            database.increment("unauthorized")
            raise HTTPException(status_code=401, detail="Invalid API key")

        body = await request.body()
        if len(body) > config.max_body_bytes:
            raise HTTPException(status_code=413, detail="Payload too large")

        content_type = request.headers.get("content-type", "").lower()
        if not content_type.startswith(("application/x-www-form-urlencoded", "multipart/form-data")):
            raise HTTPException(status_code=415, detail="Expected HTML form data")

        form = await request.form(max_fields=200, max_files=0, max_part_size=config.max_body_bytes)
        if len(form) == 1 and form.get("test") == "test":
            database.increment("received")
            database.increment("test_requests")
            return "ok"

        fields: dict[str, str | list[str]] = {}
        for key, value in form.multi_items():
            text_value = str(value)
            if key in fields:
                previous = fields[key]
                fields[key] = [*previous, text_value] if isinstance(previous, list) else [previous, text_value]
            else:
                fields[key] = text_value

        try:
            database.observe_fields(fields)
        except Exception as error:
            raise HTTPException(status_code=503, detail="Unable to persist form field catalog") from error

        event_id = uuid.uuid4().hex
        received_at = datetime.now(UTC)
        dedupe_key = cipher.dedupe(fields, event_id)
        encrypted = cipher.encrypt(event_id, fields, received_at)
        try:
            enqueue_result = database.enqueue(
                event_id=event_id,
                dedupe_key=dedupe_key,
                encrypted=encrypted,
                max_pending=config.queue_size,
                retention_seconds=config.queue_retention_seconds,
                dedupe_retention_seconds=config.dedupe_retention_seconds,
            )
        except Exception as error:
            raise HTTPException(status_code=503, detail="Unable to persist webhook") from error
        if enqueue_result.full:
            raise HTTPException(status_code=503, detail="Mail queue is full")
        if not enqueue_result.duplicate:
            worker.wake()
        return "ok"

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_page(request: Request, _: str = Depends(require_admin)):
        settings = database.get_settings()
        totals, by_day = database.stats()
        queue_stats = database.queue_stats()
        max_sent = max((int(row["sent"]) for row in by_day), default=1) or 1
        return templates.TemplateResponse(
            request=request,
            name="admin.html",
            context={
                "settings": settings,
                "totals": totals,
                "by_day": by_day,
                "max_sent": max_sent,
                "queue_depth": queue_stats.pending,
                "queue_retrying": queue_stats.retrying,
                "csrf_token": csrf_token(),
                "saved": request.query_params.get("saved") in {"1", "route"},
                "smtp_configured": bool(config.smtp_host and config.smtp_username),
                "webhook_header": config.webhook_api_key_header,
            },
        )

    @app.get("/admin/routes", response_class=HTMLResponse)
    async def routes_page(request: Request, _: str = Depends(require_admin)):
        return templates.TemplateResponse(request=request, name="routes.html", context={
            "routes": database.get_routes(), "field_catalog": database.field_catalog(),
            "saved": request.query_params.get("saved") == "1",
        })

    @app.get("/admin/routes/new", response_class=HTMLResponse)
    async def new_route_page(request: Request, _: str = Depends(require_admin)):
        return templates.TemplateResponse(request=request, name="route.html", context={
            "route": None, "settings": database.get_settings(), "csrf_token": csrf_token(),
        })

    def require_route(route_id: int):
        route = database.get_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="Маршрут не найден")
        return route

    @app.get("/admin/routes/{route_id}", response_class=HTMLResponse)
    async def route_page(route_id: int, request: Request, _: str = Depends(require_admin)):
        return templates.TemplateResponse(request=request, name="route.html", context={
            "route": require_route(route_id), "settings": database.get_settings(),
            "csrf_token": csrf_token(), "saved": request.query_params.get("saved") == "1",
        })

    @app.get("/admin/logs", response_class=HTMLResponse)
    async def logs_page(
        request: Request, _: str = Depends(require_admin),
        offset: int = Query(0, ge=0, le=1_000_000),
        recipient: str = Query("", max_length=320),
        start_date: str = Query("", max_length=10), end_date: str = Query("", max_length=10),
        delivery_status: str = Query("", pattern="^(|sent|resent|return|bounce|cancel)$"),
    ):
        try:
            start_date = date.fromisoformat(start_date).isoformat() if start_date else ""
            end_date = date.fromisoformat(end_date).isoformat() if end_date else ""
        except ValueError:
            raise HTTPException(status_code=422, detail="Некорректная дата") from None
        if start_date and end_date and start_date > end_date:
            raise HTTPException(status_code=422, detail="Дата начала позже даты окончания")
        params: dict[str, str | int] = {"limit": 50, "offset": offset}
        for key, value in (("to", recipient.strip()), ("startDate", start_date),
                           ("endDate", end_date), ("status", delivery_status)):
            if value:
                params[key] = str(value)
        rows = []
        error = ""
        if config.smtp_bz_api_key:
            try:
                rows = await asyncio.to_thread(fetch_messages, config.smtp_bz_api_key, params)
            except JournalError as failure:
                error = str(failure)
        columns, cells = journal_table(rows)
        return templates.TemplateResponse(request=request, name="logs.html", context={
            "configured": bool(config.smtp_bz_api_key), "error": error,
            "columns": columns, "cells": cells, "recipient": recipient,
            "start_date": start_date or "", "end_date": end_date or "",
            "delivery_status": delivery_status, "offset": offset,
            "previous_url": str(request.url.include_query_params(offset=max(0, offset - 50))),
            "next_url": str(request.url.include_query_params(offset=offset + 50)),
        })

    def validate_sender_name(value: str) -> str:
        if len(value) > 100 or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise HTTPException(status_code=422, detail="Имя отправителя: до 100 символов, без переносов строк")
        return value.strip()

    @app.post("/admin/settings")
    async def save_admin_settings(
        _: str = Depends(require_admin),
        csrf: str = Form(...),
        recipients: str = Form(...),
        sender_email: str = Form(...),
        sender_name: str = Form(""),
        subject_template: str = Form(...),
        email_template: str = Form(...),
    ):
        if not _secure_equal(csrf, csrf_token()):
            raise HTTPException(status_code=403, detail="Invalid CSRF token")
        try:
            parse_recipients(recipients)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        sender_email = sender_email.strip()
        if sender_email.count("@") != 1 or "\r" in sender_email or "\n" in sender_email:
            raise HTTPException(status_code=422, detail="Некорректный адрес отправителя")
        if not subject_template.strip() or not email_template.strip():
            raise HTTPException(status_code=422, detail="Шаблоны не могут быть пустыми")
        database.save_settings(
            {
                "recipients": recipients.strip(),
                "sender_email": sender_email,
                "sender_name": validate_sender_name(sender_name),
                "subject_template": subject_template.strip(),
                "email_template": email_template.strip(),
            }
        )
        return RedirectResponse("/admin?saved=1", status_code=303)

    def validate_route(
        *,
        name: str,
        match_field: str,
        match_value: str,
        recipients: str,
        subject_template: str,
        email_template: str,
    ) -> dict[str, str]:
        values = {
            "name": name.strip(),
            "match_field": match_field.strip(),
            "match_value": match_value.strip(),
            "recipients": recipients.strip(),
            "subject_template": subject_template.strip(),
            "email_template": email_template.strip(),
        }
        limits = {
            "name": 100,
            "match_field": 200,
            "match_value": 500,
            "recipients": 10_000,
            "subject_template": 1_000,
            "email_template": 100_000,
        }
        for key, value in values.items():
            if not value:
                raise HTTPException(status_code=422, detail="Все поля маршрута обязательны")
            if len(value) > limits[key]:
                raise HTTPException(status_code=422, detail=f"Поле маршрута {key} слишком длинное")
        try:
            parse_recipients(values["recipients"])
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return values

    @app.post("/admin/routes")
    async def create_mail_route(
        _: str = Depends(require_admin),
        csrf: str = Form(...),
        name: str = Form(...),
        match_field: str = Form(...),
        match_value: str = Form(...),
        recipients: str = Form(...),
        subject_template: str = Form(...),
        email_template: str = Form(...),
        enabled: str | None = Form(None),
        sender_name: str = Form(""),
    ):
        if not _secure_equal(csrf, csrf_token()):
            raise HTTPException(status_code=403, detail="Invalid CSRF token")
        values = validate_route(
            name=name,
            match_field=match_field,
            match_value=match_value,
            recipients=recipients,
            subject_template=subject_template,
            email_template=email_template,
        )
        try:
            route_id = database.create_route(
                **values, enabled=enabled == "1", sender_name=validate_sender_name(sender_name),
            )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return RedirectResponse(f"/admin/routes/{route_id}?saved=1", status_code=303)

    @app.post("/admin/routes/{route_id}")
    async def update_mail_route(
        route_id: int,
        _: str = Depends(require_admin),
        csrf: str = Form(...),
        operation: str = Form("save"),
        name: str = Form(...),
        match_field: str = Form(...),
        match_value: str = Form(...),
        recipients: str = Form(...),
        subject_template: str = Form(...),
        email_template: str = Form(...),
        enabled: str | None = Form(None),
        sender_name: str = Form(""),
    ):
        if not _secure_equal(csrf, csrf_token()):
            raise HTTPException(status_code=403, detail="Invalid CSRF token")
        require_route(route_id)
        if operation == "delete":
            database.delete_route(route_id)
            return RedirectResponse("/admin/routes?saved=1", status_code=303)
        values = validate_route(
            name=name,
            match_field=match_field,
            match_value=match_value,
            recipients=recipients,
            subject_template=subject_template,
            email_template=email_template,
        )
        try:
            database.update_route(
                route_id, **values, enabled=enabled == "1", sender_name=validate_sender_name(sender_name),
            )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return RedirectResponse(f"/admin/routes/{route_id}?saved=1", status_code=303)

    return app


app = create_app()
