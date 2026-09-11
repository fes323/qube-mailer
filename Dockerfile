FROM ghcr.io/astral-sh/uv:0.11.14 AS uv

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY --from=uv /uv /bin/uv
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
COPY README.md ./README.md
RUN uv sync --frozen --no-dev

RUN addgroup --system app && adduser --system --ingroup app app \
    && mkdir -p /data && chown app:app /data
USER app

EXPOSE 8000
CMD ["/app/.venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers", "--no-access-log"]
