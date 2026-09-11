from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from email.utils import parseaddr
from io import BytesIO
from urllib.error import HTTPError, URLError

import pytest
from fastapi.testclient import TestClient

from test_app import config
from app.database import Database
from app.main import create_app
from app.mailer import MailWorker, Submission, parse_recipients
from app.queue_crypto import QueueCipher
from app.smtp_bz import JournalError, fetch_messages, message_rows

AUTH = ('admin', 'admin-secret-at-least-32-characters')


def route_data(token: str) -> dict:
    return dict(csrf=token, name='Выставка', match_field='formid', match_value='form-x',
                recipients='one@example.test; two@example.test', sender_name='Команда выставки',
                subject_template='Заявка {{ Name }}', email_template='<p>{{ Name }}</p>', enabled='1')


def test_separate_route_pages_crud_and_sender_validation(tmp_path):
    app = create_app(config(tmp_path))
    with TestClient(app) as client:
        for url in ('/admin/routes', '/admin/routes/new', '/admin/routes/1', '/admin/logs'):
            assert client.get(url).status_code == 401
        page = client.get('/admin/routes/new', auth=AUTH)
        assert page.status_code == 200
        token = page.text.split('name="csrf" value="', 1)[1].split('"', 1)[0]
        data = route_data(token)
        assert client.post('/admin/routes', auth=AUTH, data={**data, 'csrf': 'bad'}).status_code == 403
        assert client.post('/admin/routes', auth=AUTH,
                           data={**data, 'sender_name': 'Name\r\nBcc: victim@example.test'}).status_code == 422
        response = client.post('/admin/routes', auth=AUTH, data=data, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers['location'] == '/admin/routes/1?saved=1'
        assert app.state.database.get_route(1).sender_name == 'Команда выставки'
        listing = client.get('/admin/routes', auth=AUTH).text
        assert 'href="/admin/routes/1"' in listing
        assert 'name="email_template"' not in listing
        assert 'Команда выставки' in client.get('/admin/routes/1', auth=AUTH).text
        assert client.get('/admin/routes/999', auth=AUTH).status_code == 404
        assert client.post('/admin/routes', auth=AUTH, data=data).status_code == 422
        updated = {**data, 'sender_name': '', 'name': 'Новое имя'}
        assert client.post('/admin/routes/1', auth=AUTH, data=updated).status_code == 200
        assert app.state.database.get_route(1).sender_name == ''
        assert client.post('/admin/routes/1', auth=AUTH,
                           data={**data, 'operation': 'delete', 'csrf': 'bad'}).status_code == 403
        response = client.post('/admin/routes/1', auth=AUTH,
                               data={**data, 'operation': 'delete'}, follow_redirects=False)
        assert response.headers['location'] == '/admin/routes?saved=1'
        assert app.state.database.get_routes() == []


def test_existing_database_migrates_without_losing_route(tmp_path):
    db = Database(tmp_path / 'old.db')
    db.initialize()
    values = route_data('unused')
    values.pop('csrf')
    values.pop('sender_name')
    route_id = db.create_route(**values)
    with sqlite3.connect(db.path) as connection:
        connection.execute('ALTER TABLE mail_routes DROP COLUMN sender_name')
    db.initialize()
    db.initialize()
    route = db.get_route(route_id)
    assert route.sender_name == ''
    assert route.recipients == values['recipients']
    assert route.name == values['name']


def test_each_recipient_has_independent_envelope_and_stable_retry(tmp_path, monkeypatch):
    cfg = replace(config(tmp_path), smtp_host='smtp.example.test')
    db = Database(cfg.database_path)
    db.initialize()
    db.save_settings({'sender_email': 'forms@example.test', 'sender_name': 'Общее имя',
                      'recipients': 'fallback@example.test'})
    data = route_data('unused')
    data.pop('csrf')
    data['recipients'] = 'one@example.test; two@example.test\nthree@example.test,ONE@example.test'
    route_id = db.create_route(**data)
    cipher = QueueCipher(cfg.queue_keys, cfg.active_queue_key_version, cfg.queue_dedupe_key)
    received_at = datetime.now(UTC)
    fields = {'formid': 'form-x', 'Name': 'Секретное имя'}
    dedupe = cipher.dedupe(fields, 'event-one')
    db.enqueue(event_id='event-one', dedupe_key=dedupe,
               encrypted=cipher.encrypt('event-one', fields, received_at), max_pending=10,
               retention_seconds=600, dedupe_retention_seconds=600)
    attempted = []
    accepted = []
    fail = True

    class FakeSMTP:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def starttls(self, **kwargs):
            pass
        def send_message(self, message, *, from_addr, to_addrs):
            assert len(to_addrs) == 1
            assert str(message['To']) == to_addrs[0]
            assert message.get('Cc') is None and message.get('Bcc') is None
            assert parseaddr(str(message['From'])) == ('Команда выставки', 'forms@example.test')
            assert str(message['Subject']) == 'Заявка Секретное имя'
            attempted.append((to_addrs[0], str(message['Message-ID'])))
            if fail and to_addrs == ['two@example.test']:
                return {'two@example.test': (450, b'Try later')}
            accepted.extend(to_addrs)
            return {}

    monkeypatch.setattr('app.mailer.smtplib.SMTP', FakeSMTP)
    worker = MailWorker(cfg, db, cipher)
    asyncio.run(worker._process(db.claim_next(300, 600)))
    assert accepted == ['one@example.test', 'three@example.test']
    assert db.queue_stats().pending == 1
    with sqlite3.connect(db.path) as connection:
        ciphertext = connection.execute('SELECT ciphertext FROM outbox').fetchone()[0]
        assert b'two@example.test' not in ciphertext
        assert 'Секретное имя'.encode() not in ciphertext
        connection.execute('UPDATE outbox SET available_at=0')
    # Changes after a partially successful send must not add recipients or change its content.
    db.delete_route(route_id)
    db.save_settings({'sender_name': 'Другое имя', 'recipients': 'new@example.test'})
    restarted_db = Database(db.path)
    restarted_db.initialize()
    item = restarted_db.claim_next(300, 600)
    payload = cipher.decrypt_payload(item.id, item.encrypted)
    assert payload['delivery']['pending'] == ['two@example.test']
    fail = False
    asyncio.run(MailWorker(cfg, restarted_db, cipher)._process(item))
    assert accepted == ['one@example.test', 'three@example.test', 'two@example.test']
    assert attempted[1] == attempted[-1]
    assert len(set(message_id for _, message_id in attempted)) == 3
    assert restarted_db.queue_stats().pending == 0
    assert restarted_db.stats()[0]['sent'] == 1
    assert restarted_db.stats()[0]['failed'] == 1


def test_default_sender_and_direct_multiple_recipients(tmp_path, monkeypatch):
    cfg = replace(config(tmp_path), smtp_host='smtp.example.test')
    db = Database(cfg.database_path)
    db.initialize()
    db.save_settings({'sender_email': 'forms@example.test', 'sender_name': 'Общее имя',
                      'recipients': 'one@example.test, two@example.test'})
    messages = []
    class FakeSMTP:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def starttls(self, **kwargs):
            pass
        def send_message(self, message, **kwargs):
            messages.append(message)
            return {}
    monkeypatch.setattr('app.mailer.smtplib.SMTP', FakeSMTP)
    worker = MailWorker(cfg, db, QueueCipher(cfg.queue_keys, 1, cfg.queue_dedupe_key))
    worker._send(Submission({}, datetime.now(UTC)), b'a' * 32)
    assert [str(message['To']) for message in messages] == ['one@example.test', 'two@example.test']
    assert all(parseaddr(str(message['From']))[0] == 'Общее имя' for message in messages)
    assert parse_recipients('one@example.test; ONE@example.test\ntwo@example.test') == [
        'one@example.test', 'two@example.test']


def test_logs_filters_pagination_no_store_and_safe_rendering(tmp_path, monkeypatch):
    cfg = replace(config(tmp_path), smtp_bz_api_key='private-api-key')
    app = create_app(cfg)
    calls = []
    def fake_fetch(key, params):
        calls.append((key, params))
        return [{'to': 'recipient@example.test', 'subject': '<script>unsafe</script>',
                 'status': 'sent'} for _ in range(50)]
    monkeypatch.setattr('app.main.fetch_messages', fake_fetch)
    with TestClient(app) as client:
        assert client.get('/admin/logs').status_code == 401
        assert calls == []
        response = client.get('/admin/logs', auth=AUTH, params={
            'recipient': 'recipient@example.test', 'delivery_status': 'sent',
            'start_date': '2026-09-01', 'end_date': '2026-09-11', 'offset': 50,
        })
        assert response.status_code == 200
        assert response.headers['cache-control'] == 'no-store'
        assert '&lt;script&gt;' in response.text and '<script>unsafe' not in response.text
        assert 'private-api-key' not in response.text
        assert 'offset=100' in response.text and 'offset=0' in response.text
        assert calls[-1][1] == {'limit': 50, 'offset': 50, 'to': 'recipient@example.test',
                                'startDate': '2026-09-01', 'endDate': '2026-09-11', 'status': 'sent'}
        assert client.get('/admin/logs?start_date=&end_date=', auth=AUTH).status_code == 200
        for query in ('offset=-1', 'start_date=invalid', 'start_date=2026-02-30',
                      'start_date=2026-09-11&end_date=2026-09-01', 'delivery_status=invalid'):
            assert client.get('/admin/logs?' + query, auth=AUTH).status_code == 422
    assert b'recipient@example.test' not in cfg.database_path.read_bytes()


def test_logs_missing_key_empty_and_provider_failure(tmp_path, monkeypatch):
    app = create_app(config(tmp_path))
    with TestClient(app) as client:
        response = client.get('/admin/logs', auth=AUTH)
        assert 'SMTP_BZ_API_KEY' in response.text
    app = create_app(replace(config(tmp_path), smtp_bz_api_key='secret'))
    monkeypatch.setattr('app.main.fetch_messages', lambda *_: [])
    with TestClient(app) as client:
        assert 'Писем по указанным условиям не найдено' in client.get('/admin/logs', auth=AUTH).text
        def failure(*_):
            raise JournalError('SMTP.bz недоступен. Повторите позже.')
        monkeypatch.setattr('app.main.fetch_messages', failure)
        response = client.get('/admin/logs', auth=AUTH)
        assert response.status_code == 200 and 'role="alert"' in response.text
        assert 'не найдено' not in response.text


def test_journal_client_uses_documented_authorization_and_endpoint(monkeypatch):
    class Opener:
        def open(self, request, timeout):
            assert request.full_url == 'https://api.smtp.bz/v1/log/message?limit=50&offset=0'
            assert request.get_header('Authorization') == 'key'
            assert timeout == 10
            return BytesIO(json.dumps({'data': [{'status': 'sent'}]}).encode())
    monkeypatch.setattr('app.smtp_bz.build_opener', lambda *_: Opener())
    assert fetch_messages('key', {'limit': 50, 'offset': 0}) == [{'status': 'sent'}]
    assert message_rows({'result': {'messages': []}}) == []
    with pytest.raises(JournalError):
        message_rows({'error': 'bad key'})


@pytest.mark.parametrize('code', [401, 403, 404, 429, 500, 302])
def test_journal_errors_do_not_expose_response_or_key(monkeypatch, code):
    class Opener:
        def open(self, *_args, **_kwargs):
            raise HTTPError('https://api.smtp.bz', code, 'sensitive reason', {}, BytesIO(b'sensitive body'))
    monkeypatch.setattr('app.smtp_bz.build_opener', lambda *_: Opener())
    if code == 404:
        assert fetch_messages('private-key', {}) == []
    else:
        with pytest.raises(JournalError) as error:
            fetch_messages('private-key', {})
        assert 'sensitive' not in str(error.value) and 'private-key' not in str(error.value)


@pytest.mark.parametrize('reply', [b'not json', b'x' * 2_000_001, None], ids=['invalid-json', 'oversized', 'timeout'])
def test_journal_invalid_response_and_timeout(monkeypatch, reply):
    class Opener:
        def open(self, *_args, **_kwargs):
            if reply is None:
                raise URLError('private connection details')
            return BytesIO(reply)
    monkeypatch.setattr('app.smtp_bz.build_opener', lambda *_: Opener())
    with pytest.raises(JournalError):
        fetch_messages('key', {})
