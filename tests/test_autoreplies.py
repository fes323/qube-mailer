from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from email import message_from_bytes, policy

from fastapi.testclient import TestClient

from test_app import config
from app.database import Database
from app.main import create_app
from app.mailer import MailWorker, submitter_email
from app.queue_crypto import QueueCipher
from app.templating import html_to_text

AUTH = ('admin', 'admin-secret-at-least-32-characters')
API_KEY = 'integration-key-at-least-32-characters-long'


class FakeSMTP:
    sent: list = []
    refuse: set = set()

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def starttls(self, **kwargs):
        pass

    def send_message(self, message, *, from_addr, to_addrs):
        if set(to_addrs) & self.refuse:
            return {to_addrs[0]: (550, b'No such user victim@example.test')}
        FakeSMTP.sent.append((to_addrs[0], message_from_bytes(message.as_bytes(), policy=policy.default)))
        return {}


def setup(tmp_path, monkeypatch, **overrides):
    cfg = replace(config(tmp_path), smtp_host='smtp.example.test', integration_api_key=API_KEY, **overrides)
    FakeSMTP.sent = []
    FakeSMTP.refuse = set()
    monkeypatch.setattr('app.mailer.smtplib.SMTP', FakeSMTP)
    db = Database(cfg.database_path)
    db.initialize()
    db.save_settings({'sender_email': 'forms@qube-expo.ru', 'sender_name': 'QUBE',
                      'recipients': 'staff@qube-expo.ru'})
    return cfg, db


def rule_values(**overrides):
    values = dict(name='Спасибо за заявку', enabled=True, match_field='formid', match_value='qube-feedback',
                  email_field='', sender_name='Команда QUBE', reply_to='info@qube-expo.ru',
                  subject_template='Спасибо, {{ Name }}', html_template='<p>Здравствуйте, {{ Name }}!</p>',
                  text_template='')
    values.update(overrides)
    return values


def enqueue(db, cipher, event_id, fields):
    db.enqueue(event_id=event_id, dedupe_key=cipher.dedupe(fields, event_id),
               encrypted=cipher.encrypt(event_id, fields, datetime.now(UTC)), max_pending=10,
               retention_seconds=600, dedupe_retention_seconds=600)


def csrf(client, url):
    return client.get(url, auth=AUTH).text.split('name="csrf" value="', 1)[1].split('"', 1)[0]


def test_submitter_email_detection():
    assert submitter_email({'Email': ' user@example.ru '}) == 'user@example.ru'
    assert submitter_email({'email': 'bad\r\nBcc: x@example.ru'}) is None
    assert submitter_email({'Почта': 'a@example.ru'}, 'Почта') == 'a@example.ru'
    assert submitter_email({'Email': 'a@example.ru'}, 'Почта') is None
    assert submitter_email({'Email': ['first@example.ru', 'second@example.ru']}) == 'first@example.ru'
    assert html_to_text('<html><head><style>p{}</style></head><p>Привет</p><p>Мир &amp; дом</p></html>') == 'Привет\n\nМир & дом'


def test_autoreply_sent_with_attachment_and_history(tmp_path, monkeypatch):
    cfg, db = setup(tmp_path, monkeypatch)
    rule_id = db.save_autoreply(None, rule_values())
    db.add_attachment(rule_id, 'Программа.pdf', 'application/pdf', b'%PDF-1.4 test')
    cipher = QueueCipher(cfg.queue_keys, cfg.active_queue_key_version, cfg.queue_dedupe_key)
    fields = {'formid': 'qube-feedback', 'tranid': 'qube-123', 'Name': 'Анна <b>', 'Email': 'anna@example.ru'}
    enqueue(db, cipher, 'event-1', fields)
    asyncio.run(MailWorker(cfg, db, cipher)._process(db.claim_next(300, 600)))

    assert [to for to, _ in FakeSMTP.sent] == ['staff@qube-expo.ru', 'anna@example.ru']
    reply = FakeSMTP.sent[1][1]
    assert reply['Subject'] == 'Спасибо, Анна <b>'
    assert reply['Reply-To'] == 'info@qube-expo.ru'
    assert reply['Auto-Submitted'] == 'auto-replied'
    assert 'Команда QUBE' in str(reply['From'])
    html_part = reply.get_body(('html',)).get_content()
    assert 'Анна &lt;b&gt;' in html_part
    assert reply.get_body(('plain',)).get_content().strip() == 'Здравствуйте, Анна <b>!'
    attachments = list(reply.iter_attachments())
    assert [part.get_filename() for part in attachments] == ['Программа.pdf']
    assert attachments[0].get_content() == b'%PDF-1.4 test'
    assert db.queue_stats().pending == 0

    page = db.history_page()
    assert page.total == 2
    assert {row['kind']: row['status'] for row in page.rows} == {'notification': 'sent', 'autoreply': 'sent'}
    # The history references the submission without storing the tranid or field values.
    assert db.history_page(submission_ref=cipher.reference_for('qube-123')).total == 2
    with sqlite3.connect(db.path) as connection:
        dump = '\n'.join(connection.iterdump())
    assert 'qube-123' not in dump
    assert 'Здравствуйте' in dump  # the template itself, not the letter
    assert 'Анна' not in dump.replace('Спасибо, Анна <b>', '')


def test_autoreply_specific_rule_wins_and_fallback(tmp_path, monkeypatch):
    cfg, db = setup(tmp_path, monkeypatch)
    any_id = db.save_autoreply(None, rule_values(name='Все формы', match_field='*', match_value='*'))
    specific_id = db.save_autoreply(None, rule_values(name='Подписка', match_field='form_route',
                                                      match_value='newsletter'))
    assert db.matching_autoreply({'form_route': 'newsletter'}).id == specific_id
    assert db.matching_autoreply({'formid': 'other'}).id == any_id
    db.save_autoreply(specific_id, rule_values(name='Подписка', match_field='form_route',
                                               match_value='newsletter', enabled=False))
    assert db.matching_autoreply({'form_route': 'newsletter'}).id == any_id


def test_autoreply_without_email_is_skipped_and_retry_does_not_resend(tmp_path, monkeypatch):
    cfg, db = setup(tmp_path, monkeypatch)
    db.save_autoreply(None, rule_values())
    cipher = QueueCipher(cfg.queue_keys, cfg.active_queue_key_version, cfg.queue_dedupe_key)
    enqueue(db, cipher, 'event-1', {'formid': 'qube-feedback', 'Name': 'Без почты'})
    asyncio.run(MailWorker(cfg, db, cipher)._process(db.claim_next(300, 600)))
    skipped = db.history_page(kind='autoreply').rows
    assert [(row['status'], row['error']) for row in skipped] == [('skipped', 'no_valid_email')]

    # Autoreply refused once: staff mail is not repeated, the autoreply is retried alone.
    FakeSMTP.sent = []
    FakeSMTP.refuse = {'bob@example.ru'}
    enqueue(db, cipher, 'event-2', {'formid': 'qube-feedback', 'Name': 'Боб', 'Email': 'bob@example.ru'})
    asyncio.run(MailWorker(cfg, db, cipher)._process(db.claim_next(300, 600)))
    assert [to for to, _ in FakeSMTP.sent] == ['staff@qube-expo.ru']
    row = db.history_page(kind='autoreply', recipient='bob@').rows[0]
    assert (row['status'], row['error']) == ('retrying', 'SMTPRecipientsRefused 550')
    assert 'victim' not in row['error']
    # Edits after the first attempt do not change the letter that is already being delivered.
    rule = db.matching_autoreply({'formid': 'qube-feedback'})
    db.save_autoreply(rule.id, rule_values(subject_template='Новая тема'))
    with sqlite3.connect(db.path) as connection:
        connection.execute('UPDATE outbox SET available_at=0')
    FakeSMTP.refuse = set()
    asyncio.run(MailWorker(cfg, db, cipher)._process(db.claim_next(300, 600)))
    assert [to for to, _ in FakeSMTP.sent] == ['staff@qube-expo.ru', 'bob@example.ru']
    assert FakeSMTP.sent[1][1]['Subject'] == 'Спасибо, Боб'
    row = db.history_page(kind='autoreply', recipient='bob@').rows[0]
    assert (row['status'], row['attempts']) == ('sent', 2)
    assert db.history_page(kind='notification', recipient='staff').total == 2


def test_admin_crud_upload_preview_test_send_and_history(tmp_path, monkeypatch):
    cfg, _ = setup(tmp_path, monkeypatch)
    app = create_app(cfg)
    with TestClient(app) as client:
        for url in ('/admin/autoreplies', '/admin/autoreplies/new', '/admin/history', '/admin/autoreplies/1/preview'):
            assert client.get(url, auth=('admin', 'wrong')).status_code == 401
        page = client.get('/admin/autoreplies/new?match_field=formid&match_value=qube-x&name=Тест', auth=AUTH)
        assert page.status_code == 200 and 'value="qube-x"' in page.text
        token = csrf(client, '/admin/autoreplies/new')
        data = {'csrf': token, 'name': 'Аккредитация', 'match_mode': 'form', 'match_field': 'form_route',
                'match_value': 'accreditation', 'subject_template': 'Спасибо, {{ Name }}',
                'html_template': '<p>{{ Name }}</p><script>alert(1)</script>', 'enabled': '1'}
        assert client.post('/admin/autoreplies', auth=AUTH, data={**data, 'csrf': 'bad'}).status_code == 403
        bad_file = client.post('/admin/autoreplies', auth=AUTH, data=data,
                               files=[('files', ('virus.exe', b'MZ', 'application/octet-stream'))])
        assert bad_file.status_code == 422
        response = client.post('/admin/autoreplies', auth=AUTH, data=data, follow_redirects=False,
                               files=[('files', ('../../Памятка.pdf', b'%PDF', 'text/html'))])
        assert response.status_code == 303 and response.headers['location'] == '/admin/autoreplies/1?saved=1'
        db = app.state.database
        [attachment] = db.get_attachments(1)
        assert (attachment.filename, attachment.content_type) == ('Памятка.pdf', 'application/pdf')
        assert client.post('/admin/autoreplies', auth=AUTH, data=data).status_code == 422  # same form twice

        preview = client.get('/admin/autoreplies/1/preview', auth=AUTH)
        assert '[Name]' in preview.text
        assert 'sandbox' in preview.headers['content-security-policy']
        assert preview.headers['x-frame-options'] == 'SAMEORIGIN'
        assert client.get('/admin/autoreplies/1', auth=AUTH).headers['x-frame-options'] == 'DENY'

        response = client.post('/admin/autoreplies/1/test', auth=AUTH, follow_redirects=False,
                               data={'csrf': token, 'to': 'tester@example.ru', 'values': 'Name: Тестер\nмусор'})
        assert response.status_code == 303 and 'tested=tester%40example.ru' in response.headers['location']
        to, message = FakeSMTP.sent[-1]
        assert to == 'tester@example.ru'
        assert message['Subject'] == '[Тест] Спасибо, Тестер'
        assert [part.get_filename() for part in message.iter_attachments()] == ['Памятка.pdf']
        FakeSMTP.refuse = {'down@example.ru'}
        response = client.post('/admin/autoreplies/1/test', auth=AUTH, follow_redirects=False,
                               data={'csrf': token, 'to': 'down@example.ru', 'values': ''})
        assert 'test_error=SMTPRecipientsRefused+550' in response.headers['location']
        assert client.post('/admin/autoreplies/1/test', auth=AUTH,
                           data={'csrf': token, 'to': 'not-an-email'}).status_code == 422

        history = client.get('/admin/history?kind=test', auth=AUTH)
        assert 'tester@example.ru' in history.text and 'down@example.ru' in history.text
        assert client.get('/admin/history?recipient=tester', auth=AUTH).text.count('tester@example.ru') == 1
        assert client.get('/admin/history?status=nope', auth=AUTH).status_code == 422
        assert 'Найдено: 2' in client.get('/admin/history?rule=1', auth=AUTH).text

        # Remove the attachment, then delete the rule.
        form = {**data, 'csrf': token, 'remove_attachment': str(attachment.id)}
        assert client.post('/admin/autoreplies/1', auth=AUTH, data=form).status_code == 200
        assert db.get_attachments(1) == []
        assert db.attachment_path(attachment.sha256).exists()  # kept for queued letters
        assert db.purge_attachments(0) == 1
        assert not db.attachment_path(attachment.sha256).exists()
        response = client.post('/admin/autoreplies/1', auth=AUTH, follow_redirects=False,
                               data={'csrf': token, 'operation': 'delete'})
        assert response.headers['location'] == '/admin/autoreplies?saved=1'
        assert db.get_autoreplies() == []


def test_history_purge_marks_stale_rows_expired(tmp_path, monkeypatch):
    _, db = setup(tmp_path, monkeypatch)
    old = db.history_open(delivery_key='a', kind='autoreply', recipient='x@example.ru')
    db.history_open(delivery_key='b', kind='autoreply', recipient='y@example.ru')
    with sqlite3.connect(db.path) as connection:
        connection.execute('UPDATE mail_history SET created_at=created_at-700000 WHERE id=?', (old,))
    db.purge_history(retention_seconds=10 ** 7, queue_retention_seconds=604800)
    statuses = {row['recipient']: row['status'] for row in db.history_page().rows}
    assert statuses == {'x@example.ru': 'expired', 'y@example.ru': 'queued'}
    db.purge_history(retention_seconds=600000, queue_retention_seconds=604800)
    assert db.history_page().total == 1
    assert db.history_open(delivery_key='b', kind='autoreply', recipient='y@example.ru') == 2


def test_cms_api(tmp_path, monkeypatch):
    cfg, _ = setup(tmp_path, monkeypatch)
    app = create_app(cfg)
    headers = {'Authorization': f'Bearer {API_KEY}'}
    with TestClient(app) as client:
        assert client.get('/api/v1/status').status_code == 401
        assert client.get('/api/v1/status', headers={'Authorization': 'Bearer wrong'}).status_code == 401
        status = client.get('/api/v1/status', headers=headers).json()
        assert status['ok'] and status['sender_configured']
        db = app.state.database
        db.create_route(name='Партнёры', match_field='form_route', match_value='partnership',
                        recipients='pr@qube-expo.ru', subject_template='s', email_template='e', enabled=True)
        rule_id = db.save_autoreply(None, rule_values(match_field='formid', match_value='qube-page-5-abc'))
        forms = [
            {'formid': 'qube-page-5-abc', 'formname': 'Обратная связь', 'form_route': 'feedback',
             'kind_label': 'Обратная связь', 'page_url': 'https://qube-expo.ru/contacts/'},
            {'formid': 'qube-partners', 'formname': 'Партнёрство', 'form_route': 'partnership'},
            {'formid': 'qube-partners', 'formname': 'Дубль'},
            {'formname': 'Без formid'},
        ]
        assert client.put('/api/v1/forms', json={'forms': 'x'}, headers=headers).status_code == 422
        assert client.put('/api/v1/forms', content=b'{}', headers=headers).status_code == 415
        result = client.put('/api/v1/forms', json={'forms': forms}, headers=headers).json()
        assert result['synced'] == 2
        first, second = result['forms']
        assert first['autoreply']['id'] == rule_id and first['route'] is None
        assert second['route']['name'] == 'Партнёры' and second['autoreply'] is None
        routes_page = client.get('/admin/routes', auth=AUTH).text
        assert 'qube-page-5-abc' in routes_page and 'https://qube-expo.ru/contacts/' in routes_page
        assert 'value="qube-partners"' in client.get('/admin/autoreplies/new', auth=AUTH).text

        listing = client.get('/api/v1/autoreplies', headers=headers).json()['autoreplies']
        assert listing[0]['id'] == rule_id and listing[0]['admin_path'] == f'/admin/autoreplies/{rule_id}'
        sent = client.post(f'/api/v1/autoreplies/{rule_id}/test', headers=headers,
                           json={'email': 'editor@qube-expo.ru', 'fields': {'Name': 'Редактор'},
                                 'requested_by': 'maksim'})
        assert sent.json() == {'ok': True, 'error': ''}
        assert FakeSMTP.sent[-1][1]['Subject'] == '[Тест] Спасибо, Редактор'
        assert client.post('/api/v1/autoreplies/999/test', headers=headers,
                           json={'email': 'a@example.ru'}).status_code == 404
        assert client.post(f'/api/v1/autoreplies/{rule_id}/test', headers=headers,
                           json={'email': 'bad'}).status_code == 422

        cipher = app.state.queue_cipher
        enqueue(db, cipher, 'event-1', {'formid': 'qube-page-5-abc', 'tranid': 'qube-abc',
                                        'Email': 'visitor@example.ru', 'Name': 'Гость'})
        asyncio.run(app.state.mail_worker._process(db.claim_next(300, 600)))
        history = client.get('/api/v1/history', params={'submission': 'qube-abc'}, headers=headers).json()
        assert history['total'] == 2
        assert {row['kind'] for row in history['rows']} == {'notification', 'autoreply'}
        assert 'submission_ref' not in history['rows'][0]
        tests = client.get('/api/v1/history', params={'kind': 'test'}, headers=headers).json()
        assert tests['rows'][0]['form_key'] == 'cms:maksim'


def test_api_disabled_without_key(tmp_path, monkeypatch):
    cfg, _ = setup(tmp_path, monkeypatch)
    app = create_app(replace(cfg, integration_api_key=''))
    with TestClient(app) as client:
        assert client.get('/api/v1/status', headers={'Authorization': 'Bearer '}).status_code == 404


def test_integration_key_validation(tmp_path):
    values = replace(config(tmp_path), integration_api_key='short')
    try:
        values.validate()
    except RuntimeError as error:
        assert 'INTEGRATION_API_KEY' in str(error)
    else:
        raise AssertionError('Weak integration key must be rejected')
    reused = replace(config(tmp_path), integration_api_key='webhook-secret-at-least-32-characters')
    try:
        reused.validate()
    except RuntimeError as error:
        assert 'differ' in str(error)
    else:
        raise AssertionError('Reused key must be rejected')
