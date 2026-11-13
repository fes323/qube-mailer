"""Ready-made routes for QUBE CMS forms, created once so the owner only adjusts recipients.

A preset is written to the database the first time the app starts with this code. Later edits
or deletion in «Формы и маршруты» are final: a settings flag keeps the preset from coming back.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.database import Database

LETTERS = Path(__file__).with_name("preset_letters")

# Forms whose submitter already gets a letter from the site itself (the programme PDF).
# The «all forms» autoreply would be a second letter to the same person, so it skips them.
SITE_REPLY_ROUTES = frozenset({"program_pdf"})

HOTEL_BOOKING_TEMPLATE = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Бронирование гостиницы — {{ Name }}</title>
<style type="text/css">
body, table, td, a { -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
table { border-collapse: collapse !important; border-spacing: 0 !important; }
img { border: 0; outline: none; text-decoration: none; }
body { margin: 0 !important; padding: 0 !important; background-color: #e9e8ed; }
a { color: #67269a; }
@media only screen and (max-width: 620px) {
  .pad { padding-right: 24px !important; padding-left: 24px !important; }
  .title { font-size: 26px !important; line-height: 32px !important; }
  .stack { display: block !important; width: auto !important; }
  .gap { height: 10px !important; }
}
</style>
</head>
<body style="margin:0;padding:0;background:#e9e8ed">
<div style="display:none;overflow:hidden;max-height:0;opacity:0;color:#e9e8ed;font-size:1px;line-height:1px">Новая заявка на бронирование гостиницы на QUBE EXPO: {{ Name }}, {{ Phone }}</div>
<table role="presentation" width="100%" border="0" cellpadding="0" cellspacing="0" bgcolor="#e9e8ed">
<tr><td align="center" style="padding:24px 10px 32px 10px">
<table role="presentation" width="100%" border="0" cellpadding="0" cellspacing="0" style="width:100%;max-width:650px">
  <tr>
    <td bgcolor="#1be39b" width="50%" height="6" style="height:6px;font-size:0;line-height:0">&nbsp;</td>
    <td bgcolor="#a25ffc" width="50%" height="6" style="height:6px;font-size:0;line-height:0">&nbsp;</td>
  </tr>
  <tr>
    <td colspan="2" class="pad" bgcolor="#0b1725" style="padding:26px 42px 24px 42px;border-bottom:1px solid #293044">
      <a href="https://qube-expo.ru/" target="_blank" style="text-decoration:none"><img src="https://qube-expo.ru/static/content/brand/qube-expo-logo-email.png" width="260" alt="КЬЮБ ЭКСПО | QUBE EXPO" style="display:block;width:260px;max-width:100%;height:auto;color:#ffffff;font-family:Arial,Helvetica,sans-serif;font-size:18px;font-weight:bold"></a>
    </td>
  </tr>
  <tr>
    <td colspan="2" class="pad" bgcolor="#111027" style="padding:30px 42px 32px 42px;font-family:Arial,Helvetica,sans-serif;color:#ffffff">
      <p style="margin:0 0 14px 0;color:#1be39b;font-size:13px;line-height:18px;font-weight:bold;letter-spacing:2px;text-transform:uppercase">Новая заявка &middot; бронирование гостиницы</p>
      <h1 class="title" style="margin:0;color:#ffffff;font-size:30px;line-height:38px;font-weight:bold">{{ Name }}</h1>
      <p style="margin:12px 0 0 0;color:#aeb8c2;font-size:14px;line-height:20px">Форма «{{ formname }}» на сайте QUBE EXPO</p>
    </td>
  </tr>
  <tr>
    <td colspan="2" class="pad" bgcolor="#ffffff" style="padding:30px 42px 6px 42px;font-family:Arial,Helvetica,sans-serif">
      {{ contact_buttons }}
    </td>
  </tr>
  <tr>
    <td colspan="2" class="pad" bgcolor="#ffffff" style="padding:24px 42px 8px 42px;font-family:Arial,Helvetica,sans-serif;color:#25222a">
      <p style="margin:0 0 12px 0;padding:0 0 0 12px;border-left:4px solid #a25ffc;color:#111027;font-size:17px;line-height:22px;font-weight:bold;text-transform:uppercase">Заявка</p>
      {{ answers_table }}
    </td>
  </tr>
  <tr>
    <td colspan="2" class="pad" bgcolor="#ffffff" style="padding:22px 42px 8px 42px;font-family:Arial,Helvetica,sans-serif">
      <table role="presentation" width="100%" border="0" cellpadding="0" cellspacing="0">
        <tr><td bgcolor="#f3f2f6" style="padding:16px 18px;border-left:4px solid #1be39b;color:#25222a;font-size:14px;line-height:21px">
          Гость уже получил отбивку с условиями отелей-партнёров и PDF во вложении: Radisson Blu Олимпийский, Solux Hotel Moscow и 1 Арт Отель. Нажмите «Ответить», чтобы написать гостю: ответ уйдёт на его почту.
        </td></tr>
      </table>
    </td>
  </tr>
  <tr>
    <td colspan="2" class="pad" bgcolor="#ffffff" style="padding:22px 42px 32px 42px;font-family:Arial,Helvetica,sans-serif;color:#25222a">
      <p style="margin:0 0 10px 0;padding:0 0 0 12px;border-left:4px solid #a25ffc;color:#111027;font-size:14px;line-height:20px;font-weight:bold;text-transform:uppercase">Откуда пришла заявка</p>
      {{ source_table }}
    </td>
  </tr>
  <tr>
    <td colspan="2" class="pad" bgcolor="#0b1725" style="padding:20px 42px;font-family:Arial,Helvetica,sans-serif;color:#aeb8c2;font-size:12px;line-height:18px">
      QUBE EXPO &middot; письмо для организаторов, отправлено автоматически QUBE Mailer.<br>Получено: {{ received_at }}
    </td>
  </tr>
  <tr>
    <td bgcolor="#1be39b" width="50%" height="6" style="height:6px;font-size:0;line-height:0">&nbsp;</td>
    <td bgcolor="#a25ffc" width="50%" height="6" style="height:6px;font-size:0;line-height:0">&nbsp;</td>
  </tr>
</table>
</td></tr>
</table>
</body>
</html>
"""

HOTEL_BOOKING_ROUTE = {
    "name": "Бронирование гостиницы",
    "match_field": "form_route",
    "match_value": "hotel_booking",
    # The hotel letter on the site sends guests to this address, so it gets the requests too.
    "recipients": "show.director@qube-expo.ru",
    "include_default": True,
    "subject_template": "QUBE EXPO · Бронирование гостиницы: {{ Name }}",
    "email_template": HOTEL_BOOKING_TEMPLATE,
    "sender_name": "QUBE EXPO",
}

PRESETS = {"preset_route_hotel_booking": HOTEL_BOOKING_ROUTE}

# Realistic values for the route preview of a preset.
HOTEL_BOOKING_SAMPLE: dict[str, Any] = {
    "formid": "qube-hotel-booking-form",
    "formname": "Бронирование гостиницы",
    "form_route": "hotel_booking",
    "Name": "Анна Смирнова",
    "Email": "anna.smirnova@example.ru",
    "Phone": "+7 916 000-00-00",
    "Comment": "Нужны 2 одноместных номера с 30 ноября по 3 декабря, ближе к выставке.",
    "FormAnswers": (
        '[{"key": "name", "label": "Имя", "type": "text", "value": "Анна Смирнова"},'
        ' {"key": "email", "label": "Email", "type": "email", "value": "anna.smirnova@example.ru"},'
        ' {"key": "phone", "label": "Телефон", "type": "phone", "value": "+7 916 000-00-00"},'
        ' {"key": "comment", "label": "Сообщение", "type": "textarea",'
        ' "value": "Нужны 2 одноместных номера с 30 ноября по 3 декабря, ближе к выставке."}]'
    ),
    "marketing_consent": "yes",
    "traffic_source": "Яндекс Директ",
    "utm_source": "yandex",
    "utm_medium": "cpc",
    "utm_campaign": "qube_hotels",
    "entry_path": "/visit/",
}

# Letter to the guest: the owner's text and layout moved over from the site letter (RU), with the PDF.
HOTEL_BOOKING_AUTOREPLY = {
    "name": "Бронирование гостиницы: письмо гостю",
    "enabled": True,
    "match_field": "form_route",
    "match_value": "hotel_booking",
    "email_field": "",
    "sender_name": "QUBE EXPO",
    "reply_to": "show.director@qube-expo.ru",
    "subject_template": "QUBE EXPO 2026: специальные условия бронирования в отелях-партнёрах",
    "html_file": "hotel_booking_guest.html",
    "text_template": "",
    "attachments": [("Qube_Expo_special_hotel_booking.pdf", "application/pdf")],
}

AUTOREPLY_PRESETS = {"preset_autoreply_hotel_booking": HOTEL_BOOKING_AUTOREPLY}

SAMPLES = {("form_route", "hotel_booking"): HOTEL_BOOKING_SAMPLE}


def seed_presets(connection: sqlite3.Connection) -> None:
    """Insert each preset route once; runs inside Database.initialize's transaction."""
    now = time.time()
    for flag, route in PRESETS.items():
        if connection.execute("SELECT 1 FROM settings WHERE key=?", (flag,)).fetchone():
            continue
        connection.execute("INSERT INTO settings(key, value) VALUES (?, '1')", (flag,))
        exists = connection.execute(
            "SELECT 1 FROM mail_routes WHERE match_field=? AND match_value=?",
            (route["match_field"], route["match_value"]),
        ).fetchone()
        if exists:
            continue  # The owner already set this form up by hand: keep their route.
        connection.execute(
            """
            INSERT INTO mail_routes(
                name, match_field, match_value, recipients, subject_template, email_template,
                enabled, created_at, updated_at, sender_name, include_default
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            """,
            (
                route["name"], route["match_field"], route["match_value"], route["recipients"],
                route["subject_template"], route["email_template"], now, now, route["sender_name"],
                int(route["include_default"]),
            ),
        )


def seed_autoreplies(database: Database) -> None:
    """Create each preset autoreply with its attachments once; a deleted one never comes back."""
    for flag, preset in AUTOREPLY_PRESETS.items():
        settings = database.get_settings()
        if flag in settings:
            continue
        exists = any(
            rule.match_field == preset["match_field"] and rule.match_value == preset["match_value"]
            for rule in database.get_autoreplies()
        )
        if not exists:
            values = {key: value for key, value in preset.items() if key not in {"html_file", "attachments"}}
            values["html_template"] = (LETTERS / preset["html_file"]).read_text(encoding="utf-8")
            rule_id = database.save_autoreply(None, values)
            for filename, content_type in preset["attachments"]:
                database.add_attachment(rule_id, filename, content_type, (LETTERS / filename).read_bytes())
        database.set_flag(flag)
