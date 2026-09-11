from __future__ import annotations

import html
import re
from collections.abc import Mapping
from typing import Any

PLACEHOLDER = re.compile(r"{{\s*([^\r\n{}]+?)\s*}}")


def _display_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def fields_table(fields: Mapping[str, Any]) -> str:
    rows = []
    for key, value in fields.items():
        rows.append(
            '<tr><td style="padding:9px 12px;border-bottom:1px solid #e5e7eb;'
            'font-weight:600;vertical-align:top">'
            f"{html.escape(str(key))}</td>"
            '<td style="padding:9px 12px;border-bottom:1px solid #e5e7eb;'
            'white-space:pre-wrap">'
            f"{html.escape(_display_value(value))}</td></tr>"
        )
    return '<table role="presentation" style="width:100%;border-collapse:collapse">' + "".join(rows) + "</table>"


def render_html(template: str, fields: Mapping[str, Any], received_at: str) -> str:
    special = {"fields_table": fields_table(fields), "received_at": html.escape(received_at)}

    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if key.startswith("field:"):
            return html.escape(_display_value(fields.get(key.removeprefix("field:").strip(), "")))
        if key in special:
            return special[key]
        return html.escape(_display_value(fields.get(key, "")))

    return PLACEHOLDER.sub(replace, template)


def render_subject(template: str, fields: Mapping[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if key.startswith("field:"):
            key = key.removeprefix("field:").strip()
        return _display_value(fields.get(key, ""))

    subject = PLACEHOLDER.sub(replace, template)
    return " ".join(subject.replace("\r", " ").replace("\n", " ").split())[:250]
