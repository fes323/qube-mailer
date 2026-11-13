from __future__ import annotations

import html
import json
import re
from datetime import date
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


LABEL_STYLE = (
    "padding:10px 14px 10px 0;border-bottom:1px solid #e3e1ea;color:#6b6874;font-size:13px;"
    "line-height:19px;vertical-align:top;width:38%"
)
VALUE_STYLE = (
    "padding:10px 0;border-bottom:1px solid #e3e1ea;color:#111027;font-size:15px;line-height:22px;"
    "font-weight:bold;vertical-align:top;white-space:pre-wrap;word-break:break-word"
)
STANDARD_ANSWERS = (("Name", "Имя"), ("Company", "Компания"), ("Email", "Email"), ("Phone", "Телефон"),
                    ("Comment", "Сообщение"))
SOURCE_FIELDS = (
    ("traffic_source", "Источник"), ("utm_source", "utm_source"), ("utm_medium", "utm_medium"),
    ("utm_campaign", "utm_campaign"), ("utm_content", "utm_content"), ("utm_term", "utm_term"),
    ("referrer", "Переход с сайта"), ("entry_path", "Страница входа"), ("formid", "Форма (formid)"),
)


def _answer_text(value: Any) -> str:
    if value is None or value == "" or value == []:
        return "—"
    if isinstance(value, bool):
        return "Да" if value else "Нет"
    if isinstance(value, list):
        return ", ".join(_answer_text(item) for item in value)
    if isinstance(value, dict):
        return ", ".join(f"{key}: {_answer_text(item)}" for key, item in value.items())
    text = str(value)
    try:
        return date.fromisoformat(text).strftime("%d.%m.%Y") if len(text) == 10 else text
    except ValueError:
        return text


def _rows(pairs: list[tuple[str, str]]) -> str:
    if not pairs:
        return '<p style="margin:0;color:#6b6874;font-size:14px">—</p>'
    rows = "".join(
        f'<tr><td style="{LABEL_STYLE}">{html.escape(label)}</td>'
        f'<td style="{VALUE_STYLE}">{html.escape(value)}</td></tr>'
        for label, value in pairs
    )
    return (
        '<table role="presentation" width="100%" border="0" cellpadding="0" cellspacing="0" '
        f'style="width:100%;border-collapse:collapse;font-family:Arial,Helvetica,sans-serif">{rows}</table>'
    )


def answers_table(fields: Mapping[str, Any]) -> str:
    """The questions of a QUBE CMS form with their labels (FormAnswers), in the form's order."""
    pairs: list[tuple[str, str]] = []
    try:
        answers = json.loads(str(fields.get("FormAnswers") or "[]"))
    except ValueError:
        answers = []
    for answer in answers if isinstance(answers, list) else []:
        if isinstance(answer, dict):
            label = str(answer.get("label") or answer.get("key") or "")
            pairs.append((label, _answer_text(answer.get("value"))))
    if not pairs:
        pairs = [(label, _answer_text(fields[key])) for key, label in STANDARD_ANSWERS if fields.get(key)]
    if fields.get("marketing_consent"):
        consent = "да" if str(fields["marketing_consent"]).strip() == "yes" else "нет"
        pairs.append(("Согласие на рекламную рассылку", consent))
    return _rows(pairs)


def source_table(fields: Mapping[str, Any]) -> str:
    """Traffic source and UTM tags that came with the request; empty ones are left out."""
    return _rows([(label, _display_value(fields[key])) for key, label in SOURCE_FIELDS if fields.get(key)])


def _first(fields: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = fields.get(name)
        if isinstance(value, list):
            value = value[0] if value else ""
        if str(value or "").strip():
            return str(value).strip()
    return ""


def contact_buttons(fields: Mapping[str, Any]) -> str:
    """Square buttons «write» and «call» with the submitter's e-mail and phone, when present."""
    cells = []
    email = _first(fields, "Email", "email")
    if email and EMAIL_SHAPE.fullmatch(email):
        cells.append(("#1be39b", "#0b1725", f"mailto:{email}", "Написать", email))
    phone = _first(fields, "Phone", "phone")
    digits = re.sub(r"[^0-9+]", "", phone)
    if phone and len(digits) >= 5:
        cells.append(("#111027", "#ffffff", f"tel:{digits}", "Позвонить", phone))
    if not cells:
        return ""
    parts = []
    for index, (background, color, href, caption, value) in enumerate(cells):
        gap = '<td class="stack gap" width="12" style="width:12px;font-size:0;line-height:0">&nbsp;</td>' if index else ""
        parts.append(
            f'{gap}<td class="stack" bgcolor="{background}" style="padding:14px 18px;vertical-align:top">'
            f'<a href="{html.escape(href)}" style="display:block;color:{color};text-decoration:none;'
            'font-family:Arial,Helvetica,sans-serif">'
            f'<span style="display:block;font-size:11px;line-height:14px;letter-spacing:2px;'
            f'text-transform:uppercase;opacity:.8">{caption}</span>'
            f'<span style="display:block;margin-top:4px;font-size:16px;line-height:22px;font-weight:bold;'
            f'word-break:break-all">{html.escape(value)}</span></a></td>'
        )
    return (
        '<table role="presentation" width="100%" border="0" cellpadding="0" cellspacing="0">'
        f'<tr>{"".join(parts)}</tr></table>'
    )


EMAIL_SHAPE = re.compile(r"[^@\s<>\"']{1,64}@[A-Za-z0-9.-]{1,252}\.[A-Za-z0-9-]{2,63}")


def render_html(
    template: str, fields: Mapping[str, Any], received_at: str, *, show_missing: bool = False,
) -> str:
    """``show_missing`` renders unknown variables as [name]: used by previews only."""
    special = {
        "fields_table": fields_table(fields),
        "received_at": html.escape(received_at),
        "answers_table": answers_table(fields),
        "source_table": source_table(fields),
        "contact_buttons": contact_buttons(fields),
    }

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
