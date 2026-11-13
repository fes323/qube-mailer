from __future__ import annotations

import asyncio
import json

from fastapi.testclient import TestClient
from test_app import config
from test_autoreplies import AUTH, FakeSMTP, enqueue, rule_values, setup

from app.database import Database
from app.mailer import MailWorker
from app.main import create_app
from app.presets import HOTEL_BOOKING_SAMPLE
from app.queue_crypto import QueueCipher
from app.templating import answers_table, contact_buttons, render_html, source_table


def hotel_route(db):
    return db.matching_route({'form_route': 'hotel_booking'})


def test_hotel_route_created_once_and_never_resurrected(tmp_path):
    db = Database(tmp_path / 'presets.db')
    db.initialize()
    route = hotel_route(db)
    assert route.name == 'Бронирование гостиницы'
    assert route.recipients == 'show.director@qube-expo.ru'
    assert route.include_default and route.enabled
    assert route.sender_name == 'QUBE EXPO'
    assert 'QUBE EXPO' in route.subject_template
    assert 'border-radius' not in route.email_template
    db.delete_route(route.id)
    db.initialize()
    assert hotel_route(db) is None


def test_hand_made_route_is_kept(tmp_path):
    db = Database(tmp_path / 'presets.db')
    db.initialize()
    db.delete_route(hotel_route(db).id)
    with db._connect() as connection:
        connection.execute("DELETE FROM settings WHERE key='preset_route_hotel_booking'")
    db.create_route(name='Мой', match_field='form_route', match_value='hotel_booking',
                    recipients='me@example.test', subject_template='S', email_template='<p></p>', enabled=True)
    db.initialize()
    assert [route.name for route in db.get_routes()] == ['Мой']


def test_hotel_request_staff_letters_and_guest_letter_with_pdf(tmp_path, monkeypatch):
    cfg, db = setup(tmp_path, monkeypatch)
    db.save_settings({'recipients': 'staff@qube-expo.ru, show.director@qube-expo.ru'})
    db.save_autoreply(None, rule_values(name='Все формы', match_field='*', match_value='*'))
    cipher = QueueCipher(cfg.queue_keys, cfg.active_queue_key_version, cfg.queue_dedupe_key)
    enqueue(db, cipher, 'event-hotel', dict(HOTEL_BOOKING_SAMPLE, tranid='qube-1'))
    asyncio.run(MailWorker(cfg, db, cipher)._process(db.claim_next(300, 600)))

    # Route address first, then the defaults; the shared address gets one letter; then the guest.
    assert [to for to, _ in FakeSMTP.sent] == [
        'show.director@qube-expo.ru', 'staff@qube-expo.ru', 'anna.smirnova@example.ru',
    ]
    letter = FakeSMTP.sent[0][1]
    assert letter['Subject'] == 'QUBE EXPO · Бронирование гостиницы: Анна Смирнова'
    assert letter['Reply-To'] == 'anna.smirnova@example.ru'
    assert 'QUBE EXPO' in str(letter['From'])
    body = letter.get_body(('html',)).get_content()
    assert 'mailto:anna.smirnova@example.ru' in body and 'tel:+79160000000' in body
    assert 'Сообщение' in body and 'Яндекс Директ' in body

    guest = FakeSMTP.sent[2][1]
    assert guest['Subject'] == 'QUBE EXPO 2026: специальные условия бронирования в отелях-партнёрах'
    assert guest['Reply-To'] == 'show.director@qube-expo.ru'
    html_part = guest.get_body(('html',)).get_content()
    for text in ('RADISSON BLU ОЛИМПИЙСКИЙ, МОСКВА', 'SOLUX HOTEL MOSCOW', '1 АРТ ОТЕЛЬ 4*', 'Юлия Игнатьева',
                 'QUBE2026'):
        assert text in html_part
    assert '{%' not in html_part and '{{' not in html_part
    attachments = list(guest.iter_attachments())
    assert [part.get_filename() for part in attachments] == ['Qube_Expo_special_hotel_booking.pdf']
    assert attachments[0].get_content().startswith(b'%PDF')
    rows = db.history_page().rows
    assert sorted(row['kind'] for row in rows) == ['autoreply', 'notification', 'notification']
    assert {row['rule_name'] for row in rows if row['kind'] == 'autoreply'} == {'Бронирование гостиницы: письмо гостю'}


def test_guest_autoreply_created_once(tmp_path):
    db = Database(tmp_path / 'presets.db')
    db.initialize()
    rule = db.matching_autoreply({'form_route': 'hotel_booking'})
    assert rule.sender_name == 'QUBE EXPO' and rule.enabled
    assert [item.filename for item in db.get_attachments(rule.id)] == ['Qube_Expo_special_hotel_booking.pdf']
    db.delete_autoreply(rule.id)
    db.initialize()
    assert db.matching_autoreply({'form_route': 'hotel_booking'}) is None


def test_site_reply_forms_skip_the_fallback(tmp_path, monkeypatch):
    _, db = setup(tmp_path, monkeypatch)
    any_id = db.save_autoreply(None, rule_values(name='Все', match_field='*', match_value='*'))
    assert db.matching_autoreply({'form_route': 'program_pdf'}) is None
    assert db.matching_autoreply({'form_route': 'feedback'}).id == any_id
    own = db.save_autoreply(None, rule_values(match_field='form_route', match_value='program_pdf'))
    assert db.matching_autoreply({'form_route': 'program_pdf'}).id == own


def test_cms_blocks_escape_and_format():
    answers = [
        {'label': 'Заезд', 'value': '2026-11-30'},
        {'label': 'Завтрак', 'value': True},
        {'label': 'Гости <script>', 'value': ['A', 'B&C']},
        {'label': 'Пусто', 'value': ''},
    ]
    table = answers_table({'FormAnswers': json.dumps(answers), 'marketing_consent': 'no'})
    assert '30.11.2026' in table and 'Да' in table and 'A, B&amp;C' in table
    assert '<script>' not in table and '—' in table and 'Согласие на рекламную рассылку' in table
    # No FormAnswers (older forms): the standard fields.
    assert 'Компания' in answers_table({'Name': 'Иван', 'Company': 'ООО'})
    assert answers_table({'FormAnswers': 'not json', 'Email': 'a@b.ru'}).count('<tr>') == 1
    assert contact_buttons({'Email': 'x"onmouseover=1@b.ru', 'Phone': '12'}) == ''
    assert 'utm_source' not in source_table({'utm_medium': 'cpc'})
    assert '{{' not in render_html('{{ answers_table }}{{ contact_buttons }}{{ source_table }}', {}, 'now')


def test_route_preview_and_include_default_form(tmp_path, monkeypatch):
    app = create_app(config(tmp_path))
    with TestClient(app) as client:
        route = hotel_route(app.state.database)
        assert client.get(f'/admin/routes/{route.id}/preview').status_code == 401
        preview = client.get(f'/admin/routes/{route.id}/preview', auth=AUTH)
        assert preview.status_code == 200
        assert 'sandbox' in preview.headers['content-security-policy']
        assert 'Анна Смирнова' in preview.text and 'QUBE EXPO' in preview.text
        page = client.get(f'/admin/routes/{route.id}', auth=AUTH).text
        assert 'name="include_default" value="1" checked' in page
        token = page.split('name="csrf" value="', 1)[1].split('"', 1)[0]
        data = dict(csrf=token, name=route.name, match_field=route.match_field, match_value=route.match_value,
                    recipients='hotels@example.test', subject_template=route.subject_template,
                    email_template=route.email_template, enabled='1')
        assert client.post(f'/admin/routes/{route.id}', auth=AUTH, data=data,
                           follow_redirects=False).status_code == 303
        saved = app.state.database.get_route(route.id)
        assert saved.recipients == 'hotels@example.test' and not saved.include_default
        assert '+ по умолчанию' not in client.get('/admin/routes', auth=AUTH).text


def test_route_without_recipients_sends_only_the_guest_letter(tmp_path, monkeypatch):
    cfg, db = setup(tmp_path, monkeypatch)
    route = hotel_route(db)
    db.update_route(route.id, name=route.name, match_field=route.match_field, match_value=route.match_value,
                    recipients='', subject_template=route.subject_template, email_template=route.email_template,
                    enabled=True, sender_name=route.sender_name, include_default=False)
    cipher = QueueCipher(cfg.queue_keys, cfg.active_queue_key_version, cfg.queue_dedupe_key)
    enqueue(db, cipher, 'event-silent', dict(HOTEL_BOOKING_SAMPLE, tranid='qube-2'))
    asyncio.run(MailWorker(cfg, db, cipher)._process(db.claim_next(300, 600)))
    assert [to for to, _ in FakeSMTP.sent] == ['anna.smirnova@example.ru']
    assert db.queue_stats().pending == 0


def test_admin_saves_route_with_empty_recipients(tmp_path):
    app = create_app(config(tmp_path))
    with TestClient(app) as client:
        route = hotel_route(app.state.database)
        page = client.get(f'/admin/routes/{route.id}', auth=AUTH).text
        token = page.split('name="csrf" value="', 1)[1].split('"', 1)[0]
        data = {'csrf': token, 'name': route.name, 'match_field': route.match_field,
                'match_value': route.match_value, 'recipients': '', 'subject_template': route.subject_template,
                'email_template': route.email_template, 'enabled': '1'}
        assert client.post(f'/admin/routes/{route.id}', auth=AUTH, data=data,
                           follow_redirects=False).status_code == 303
        assert app.state.database.get_route(route.id).recipients == ''
        data['name'] = ''
        assert client.post(f'/admin/routes/{route.id}', auth=AUTH, data=data).status_code == 422
