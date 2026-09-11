"""Read-only SMTP.BZ journal client. Responses are never persisted or logged."""
from __future__ import annotations

import json
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener


class JournalError(Exception):
    pass


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward the API key to another endpoint.


def message_rows(payload: object) -> list[dict]:
    # The public Swagger describes query parameters but has no response schema.
    # Keep provider field names rather than guessing which date/status fields exist.
    for _ in range(4):
        if isinstance(payload, list) and all(isinstance(row, dict) for row in payload):
            return payload
        if isinstance(payload, dict):
            for key in ("data", "messages", "items", "result"):
                if isinstance(payload.get(key), (list, dict)):
                    payload = payload[key]
                    break
            else:
                break
        else:
            break
    raise JournalError("SMTP.bz вернул неизвестный формат журнала. Проверьте версию API.")


def fetch_messages(api_key: str, params: dict[str, str | int]) -> list[dict]:
    request = Request(
        "https://api.smtp.bz/v1/log/message?" + urlencode(params),
        headers={"Authorization": api_key, "Accept": "application/json"},
    )
    try:
        with build_opener(NoRedirects).open(request, timeout=10) as response:
            raw = response.read(2_000_001)
        if len(raw) > 2_000_000:
            raise JournalError("Ответ SMTP.bz слишком большой. Уточните фильтры.")
        return message_rows(json.loads(raw))
    except HTTPError as error:
        code = error.code
        error.close()
        if code == 404:
            return []
        if code in {401, 403}:
            raise JournalError("SMTP.bz отклонил API-ключ. Проверьте SMTP_BZ_API_KEY.") from None
        if code == 429:
            raise JournalError("Превышен лимит запросов SMTP.bz. Повторите позже.") from None
        raise JournalError("SMTP.bz не смог вернуть журнал. Повторите позже.") from None
    except (URLError, TimeoutError, OSError, HTTPException):
        raise JournalError("SMTP.bz недоступен. Повторите позже.") from None
    except (ValueError, UnicodeError):
        raise JournalError("SMTP.bz вернул некорректный ответ.") from None


def fetch_message_page(api_key: str, params: dict[str, str | int]) -> list[dict]:
    """Load one page in bounded batches, preserving the provider's journal order."""
    limit = int(params.get("limit", 100))
    offset = int(params.get("offset", 0))
    rows: list[dict] = []
    while len(rows) < limit:
        batch_size = min(100, limit - len(rows))
        batch = fetch_messages(api_key, {**params, "limit": batch_size, "offset": offset + len(rows)})
        rows.extend(batch[:batch_size])
        if len(batch) < batch_size:
            break
    return rows


def journal_table(rows: list[dict]) -> tuple[list[str], list[list[str]]]:
    columns = list(dict.fromkeys(str(key) for row in rows for key in row))
    values = []
    for row in rows:
        cells = []
        for column in columns:
            value = row.get(column)
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            cells.append("—" if value is None else str(value))
        values.append(cells)
    return columns, values
