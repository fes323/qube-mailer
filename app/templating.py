from __future__ import annotations

import html
import re
from html.parser import HTMLParser
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


def render_html(
    template: str, fields: Mapping[str, Any], received_at: str, *, show_missing: bool = False,
) -> str:
    """``show_missing`` renders unknown variables as [name]: used by previews only."""
    special = {"fields_table": fields_table(fields), "received_at": html.escape(received_at)}

    def value(key: str) -> str:
        if key not in fields and show_missing:
            return html.escape(f"[{key}]")
        return html.escape(_display_value(fields.get(key, "")))

    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if key.startswith("field:"):
            return value(key.removeprefix("field:").strip())
        if key in special:
            return special[key]
        return value(key)

    return PLACEHOLDER.sub(replace, template)


def render_subject(template: str, fields: Mapping[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if key.startswith("field:"):
            key = key.removeprefix("field:").strip()
        return _display_value(fields.get(key, ""))

    subject = PLACEHOLDER.sub(replace, template)
    return " ".join(subject.replace("\r", " ").replace("\n", " ").split())[:250]


def render_text(template: str, fields: Mapping[str, Any], received_at: str) -> str:
    """Plain-text alternative: no escaping, line breaks preserved, CR removed."""

    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if key == "received_at":
            return received_at
        if key == "fields_table":
            return "\n".join(f"{name}: {_display_value(item)}" for name, item in fields.items())
        if key.startswith("field:"):
            key = key.removeprefix("field:").strip()
        return _display_value(fields.get(key, ""))

    return PLACEHOLDER.sub(replace, template).replace("\r", "")


class _TextExtractor(HTMLParser):
    BLOCKS = {"p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "table", "section"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip = 0

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        if tag in {"style", "script", "head", "title"}:
            self.skip += 1
        elif tag in self.BLOCKS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"style", "script", "head", "title"}:
            self.skip = max(0, self.skip - 1)
        elif tag in self.BLOCKS or tag == "td":
            self.parts.append("\n" if tag in self.BLOCKS else " ")

    def handle_data(self, data: str) -> None:
        if not self.skip:
            self.parts.append(data)


def html_to_text(markup: str) -> str:
    parser = _TextExtractor()
    parser.feed(markup)
    parser.close()
    lines = [" ".join(line.split()) for line in "".join(parser.parts).splitlines()]
    text = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", text).strip()
