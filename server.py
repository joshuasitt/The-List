import os
import json
import sqlite3
import urllib.request
import urllib.parse
from datetime import datetime
from flask import Flask, request, jsonify, render_template, redirect, send_from_directory
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
_HERE = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.environ.get('DATA_DIR', _HERE)
DB_PATH = os.path.join(_DATA_DIR, 'data.db')


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            notes TEXT DEFAULT '',
            area TEXT DEFAULT 'General',
            scheduled_date TEXT,
            completed INTEGER DEFAULT 0,
            completed_at TEXT,
            sort_order INTEGER DEFAULT 0,
            time_estimate TEXT DEFAULT '',
            task_type TEXT DEFAULT 'main',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ideas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT DEFAULT '',
            summary TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS meetings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            meeting_date TEXT DEFAULT '',
            attendees TEXT DEFAULT '[]',
            notes TEXT DEFAULT '',
            action_items TEXT DEFAULT '[]',
            email_draft TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS week_goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start TEXT NOT NULL UNIQUE,
            goal TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );
    ''')
    conn.commit()
    conn.close()
    # migrate existing db
    conn = get_db()
    for col, ddl in [
        ('time_estimate', 'TEXT DEFAULT ""'),
        ('task_type', 'TEXT DEFAULT "main"'),
        ('start_time', 'TEXT DEFAULT ""'),
        ('duration_minutes', 'INTEGER DEFAULT 60'),
        ('gcal_event_id', 'TEXT DEFAULT ""'),
        ('archived_week', 'TEXT DEFAULT NULL'),
    ]:
        try:
            conn.execute(f'ALTER TABLE tasks ADD COLUMN {col} {ddl}')
            conn.commit()
        except Exception:
            pass
    conn.close()


def claude(prompt, max_tokens=1024):
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError('ANTHROPIC_API_KEY not set in .env file')
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model='claude-opus-4-6',
            max_tokens=max_tokens,
            messages=[{'role': 'user', 'content': prompt}]
        )
        return msg.content[0].text
    except ImportError:
        raise ValueError('Run: pip install anthropic')


# ── TASKS ──────────────────────────────────────────────────────────────────

@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    date = request.args.get('date')
    week_start = request.args.get('week_start')
    conn = get_db()
    arc = '(archived_week IS NULL OR archived_week = "")'
    if date:
        rows = conn.execute(
            f'SELECT * FROM tasks WHERE scheduled_date = ? AND {arc} ORDER BY sort_order, id',
            (date,)
        ).fetchall()
    elif week_start:
        rows = conn.execute(
            f'''SELECT * FROM tasks
               WHERE ((scheduled_date >= ? AND scheduled_date <= date(?, "+6 days"))
                  OR scheduled_date IS NULL OR scheduled_date = "")
               AND {arc}
               ORDER BY scheduled_date, sort_order, id''',
            (week_start, week_start)
        ).fetchall()
    else:
        rows = conn.execute(
            f'SELECT * FROM tasks WHERE {arc} ORDER BY scheduled_date, sort_order, id'
        ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/tasks', methods=['POST'])
def create_task():
    d = request.json
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO tasks (title, notes, area, scheduled_date, task_type, time_estimate, start_time, duration_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (d['title'], d.get('notes', ''), d.get('area', 'General'), d.get('scheduled_date') or None,
         d.get('task_type', 'main'), d.get('time_estimate', ''),
         d.get('start_time', ''), d.get('duration_minutes') or None)
    )
    task = dict(conn.execute('SELECT * FROM tasks WHERE id=?', (cur.lastrowid,)).fetchone())
    conn.commit()
    conn.close()
    return jsonify(task), 201


@app.route('/api/tasks/<int:tid>', methods=['PUT'])
def update_task(tid):
    d = request.json
    fields, vals = [], []
    for f in ('title', 'notes', 'area', 'scheduled_date', 'sort_order', 'time_estimate', 'task_type', 'start_time', 'duration_minutes'):
        if f in d:
            fields.append(f'{f} = ?')
            vals.append(d[f] if d[f] != '' or f != 'scheduled_date' else None)
    if 'completed' in d:
        fields.append('completed = ?')
        vals.append(1 if d['completed'] else 0)
        fields.append('completed_at = ?')
        vals.append(datetime.now().isoformat() if d['completed'] else None)
    vals.append(tid)
    conn = get_db()
    conn.execute(f'UPDATE tasks SET {", ".join(fields)} WHERE id=?', vals)
    task = dict(conn.execute('SELECT * FROM tasks WHERE id=?', (tid,)).fetchone())
    conn.commit()
    conn.close()
    return jsonify(task)


@app.route('/api/tasks/<int:tid>', methods=['DELETE'])
def delete_task(tid):
    conn = get_db()
    conn.execute('DELETE FROM tasks WHERE id=?', (tid,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


# ── IDEAS ──────────────────────────────────────────────────────────────────

@app.route('/api/ideas', methods=['GET'])
def get_ideas():
    conn = get_db()
    rows = conn.execute('SELECT * FROM ideas ORDER BY created_at DESC').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/ideas', methods=['POST'])
def create_idea():
    conn = get_db()
    cur = conn.execute('INSERT INTO ideas (content) VALUES (?)', (request.json['content'],))
    idea = dict(conn.execute('SELECT * FROM ideas WHERE id=?', (cur.lastrowid,)).fetchone())
    conn.commit()
    conn.close()
    return jsonify(idea), 201


@app.route('/api/ideas/<int:iid>', methods=['DELETE'])
def delete_idea(iid):
    conn = get_db()
    conn.execute('DELETE FROM ideas WHERE id=?', (iid,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


# ── ARTICLES ───────────────────────────────────────────────────────────────

@app.route('/api/articles', methods=['GET'])
def get_articles():
    conn = get_db()
    rows = conn.execute('SELECT * FROM articles ORDER BY created_at DESC').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/articles/summarize', methods=['POST'])
def summarize_article():
    d = request.json
    url = d.get('url', '').strip()
    text = d.get('text', '').strip()

    if url and not text:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read().decode('utf-8', errors='ignore')
            # strip HTML tags simply
            import re
            text = re.sub(r'<[^>]+>', ' ', raw)
            text = re.sub(r'\s+', ' ', text).strip()[:8000]
        except Exception as e:
            text = f'Could not fetch URL: {e}'

    prompt = f"""Summarize this article briefly. Format:

**Main point:** 1–2 sentences.

**Key takeaways:**
- bullet
- bullet
- bullet

**Why it matters for a lifestyle/fashion brand founder:**
1 sentence.

Content:
{text[:8000]}"""

    try:
        summary = claude(prompt)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    conn = get_db()
    cur = conn.execute('INSERT INTO articles (url, summary) VALUES (?, ?)', (url, summary))
    article = dict(conn.execute('SELECT * FROM articles WHERE id=?', (cur.lastrowid,)).fetchone())
    conn.commit()
    conn.close()
    return jsonify(article), 201


@app.route('/api/articles/<int:aid>', methods=['DELETE'])
def delete_article(aid):
    conn = get_db()
    conn.execute('DELETE FROM articles WHERE id=?', (aid,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


# ── MEETINGS ───────────────────────────────────────────────────────────────

@app.route('/api/meetings', methods=['GET'])
def get_meetings():
    conn = get_db()
    rows = conn.execute(
        'SELECT * FROM meetings ORDER BY meeting_date DESC, created_at DESC'
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/meetings', methods=['POST'])
def create_meeting():
    d = request.json
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO meetings (title, meeting_date, attendees, notes) VALUES (?, ?, ?, ?)',
        (d['title'], d.get('meeting_date', ''),
         json.dumps(d.get('attendees', [])), d.get('notes', ''))
    )
    m = dict(conn.execute('SELECT * FROM meetings WHERE id=?', (cur.lastrowid,)).fetchone())
    conn.commit()
    conn.close()
    return jsonify(m), 201


@app.route('/api/meetings/<int:mid>', methods=['PUT'])
def update_meeting(mid):
    d = request.json
    fields, vals = [], []
    for f in ('title', 'meeting_date', 'notes', 'email_draft'):
        if f in d:
            fields.append(f'{f} = ?')
            vals.append(d[f])
    for f in ('attendees', 'action_items'):
        if f in d:
            fields.append(f'{f} = ?')
            v = d[f]
            vals.append(json.dumps(v) if isinstance(v, list) else v)
    if not fields:
        return jsonify({'ok': True})
    vals.append(mid)
    conn = get_db()
    conn.execute(f'UPDATE meetings SET {", ".join(fields)} WHERE id=?', vals)
    m = dict(conn.execute('SELECT * FROM meetings WHERE id=?', (mid,)).fetchone())
    conn.commit()
    conn.close()
    return jsonify(m)


@app.route('/api/meetings/<int:mid>/extract-actions', methods=['POST'])
def extract_actions(mid):
    conn = get_db()
    m = dict(conn.execute('SELECT * FROM meetings WHERE id=?', (mid,)).fetchone())
    conn.close()

    prompt = f"""Extract specific action items from these meeting notes.
Return ONLY a JSON array of short action strings. Example: ["Josh to send brand deck to Liz", "Liz to send deal sheet by Friday"]

Meeting: {m['title']}
Notes: {m.get('notes', '')}

JSON array:"""

    try:
        result = claude(prompt, max_tokens=512)
        # find the JSON array in the response
        import re
        match = re.search(r'\[.*\]', result, re.DOTALL)
        actions = json.loads(match.group()) if match else [result.strip()]
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    conn = get_db()
    conn.execute('UPDATE meetings SET action_items=? WHERE id=?', (json.dumps(actions), mid))
    conn.commit()
    conn.close()
    return jsonify({'action_items': actions})


@app.route('/api/meetings/<int:mid>/generate-email', methods=['POST'])
def generate_email(mid):
    conn = get_db()
    m = dict(conn.execute('SELECT * FROM meetings WHERE id=?', (mid,)).fetchone())
    conn.close()

    attendees = json.loads(m['attendees']) if isinstance(m['attendees'], str) else m['attendees']
    actions = json.loads(m['action_items']) if isinstance(m['action_items'], str) else m['action_items']
    attendee_str = ', '.join(attendees) if attendees else 'unknown'

    is_internal = all('@brunch.us' in a.lower() for a in attendees if '@' in a) if attendees else False
    is_candidate = any(
        a.lower().endswith(('@gmail.com', '@yahoo.com', '@hotmail.com', '@outlook.com', '@icloud.com'))
        for a in attendees if '@' in a
    ) if attendees else False

    if is_internal:
        tone = 'Ultra brief. 1-3 sentences. No greeting. Just the action items or key takeaway.'
    elif is_candidate:
        tone = 'Warm and genuine but brief. Short paragraphs. Mention something specific you liked. Sign off "Best, Joshua" or "Warmly, Joshua".'
    else:
        tone = 'Professional, warm, brief. Short paragraphs. "Hi [First name]," opener. Clear next steps. Sign "Best, Joshua".'

    prompt = f"""Write a follow-up email from Joshua Sitt (Co-Founder, Brunch) after this meeting.

Meeting: {m['title']}
Date: {m.get('meeting_date', 'recent')}
With: {attendee_str}
Notes: {m.get('notes', '')}
Action items: {json.dumps(actions)}

Tone: {tone}

Joshua's style rules:
- Short. No filler. No "Hope this email finds you well."
- Internal to team: single sentences, no pleasantries ("Update?", "Can you check X?")
- External partners: warm but direct, clear next steps, one question max
- Candidates: genuine, specific about what he liked, clear on timeline
- Never uses corporate jargon
- Signs externally: "Best, Joshua" or "Warmly, Joshua"

Write ONLY the email body. No subject line."""

    try:
        draft = claude(prompt, max_tokens=512)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    conn = get_db()
    conn.execute('UPDATE meetings SET email_draft=? WHERE id=?', (draft, mid))
    conn.commit()
    conn.close()
    return jsonify({'email_draft': draft})


@app.route('/api/meetings/<int:mid>', methods=['DELETE'])
def delete_meeting(mid):
    conn = get_db()
    conn.execute('DELETE FROM meetings WHERE id=?', (mid,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


# ── REMINDERS ─────────────────────────────────────────────────────────────

@app.route('/api/reminders', methods=['GET'])
def get_reminders():
    conn = get_db()
    rows = conn.execute('SELECT * FROM reminders ORDER BY created_at').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/reminders', methods=['POST'])
def create_reminder():
    conn = get_db()
    cur = conn.execute('INSERT INTO reminders (content) VALUES (?)', (request.json['content'],))
    r = dict(conn.execute('SELECT * FROM reminders WHERE id=?', (cur.lastrowid,)).fetchone())
    conn.commit()
    conn.close()
    return jsonify(r), 201

@app.route('/api/reminders/<int:rid>', methods=['DELETE'])
def delete_reminder(rid):
    conn = get_db()
    conn.execute('DELETE FROM reminders WHERE id=?', (rid,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


# ── WEEK GOALS ─────────────────────────────────────────────────────────────

@app.route('/api/week-goals/<week_start>', methods=['GET'])
def get_week_goal(week_start):
    conn = get_db()
    row = conn.execute('SELECT * FROM week_goals WHERE week_start=?', (week_start,)).fetchone()
    conn.close()
    return jsonify(dict(row) if row else {'week_start': week_start, 'goal': ''})

@app.route('/api/week-goals/<week_start>', methods=['PUT'])
def set_week_goal(week_start):
    goal = request.json.get('goal', '')
    conn = get_db()
    conn.execute('INSERT INTO week_goals (week_start, goal) VALUES (?, ?) ON CONFLICT(week_start) DO UPDATE SET goal=?',
                 (week_start, goal, goal))
    conn.commit()
    conn.close()
    return jsonify({'week_start': week_start, 'goal': goal})


# ── FRONTEND ───────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/set-api-key', methods=['POST'])
def set_api_key():
    key = request.json.get('key', '').strip()
    if not key:
        return jsonify({'ok': False}), 400
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    lines = []
    if os.path.exists(env_path):
        with open(env_path) as f:
            lines = [l for l in f.readlines() if not l.startswith('ANTHROPIC_API_KEY=')]
    lines.append(f'ANTHROPIC_API_KEY={key}\n')
    with open(env_path, 'w') as f:
        f.writelines(lines)
    os.environ['ANTHROPIC_API_KEY'] = key
    return jsonify({'ok': True})


# ── GOOGLE CALENDAR ────────────────────────────────────────────────────────

GCAL_AUTH_URL  = 'https://accounts.google.com/o/oauth2/v2/auth'
GCAL_TOKEN_URL = 'https://oauth2.googleapis.com/token'
GCAL_API_URL   = 'https://www.googleapis.com/calendar/v3'
GCAL_REDIRECT  = 'http://localhost:8000/auth/google/callback'
GCAL_SCOPE     = 'https://www.googleapis.com/auth/calendar.events'
GCAL_TOKEN_PATH = os.path.join(_DATA_DIR, 'calendar_token.json')


def _gcal_token():
    if os.path.exists(GCAL_TOKEN_PATH):
        with open(GCAL_TOKEN_PATH) as f:
            return json.load(f)
    return None


def _save_gcal_token(data):
    with open(GCAL_TOKEN_PATH, 'w') as f:
        json.dump(data, f)


def _gcal_get_req(url, access_token):
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Bearer {access_token}')
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read()), r.status


def _gcal_put(url, payload, access_token):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method='PUT')
    req.add_header('Authorization', f'Bearer {access_token}')
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read()), r.status


def _gcal_post(url, payload, access_token):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Authorization', f'Bearer {access_token}')
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read()), r.status


def _refresh_gcal_token(token):
    client_id = os.getenv('GOOGLE_CLIENT_ID', '')
    client_secret = os.getenv('GOOGLE_CLIENT_SECRET', '')
    data = urllib.parse.urlencode({
        'client_id': client_id, 'client_secret': client_secret,
        'refresh_token': token.get('refresh_token', ''),
        'grant_type': 'refresh_token'
    }).encode()
    req = urllib.request.Request(GCAL_TOKEN_URL, data=data, method='POST')
    with urllib.request.urlopen(req) as r:
        new_token = json.loads(r.read())
    if 'refresh_token' not in new_token:
        new_token['refresh_token'] = token.get('refresh_token')
    _save_gcal_token(new_token)
    return new_token


@app.route('/api/set-gcal-credentials', methods=['POST'])
def set_gcal_credentials():
    d = request.json
    client_id = d.get('client_id', '').strip()
    client_secret = d.get('client_secret', '').strip()
    if not client_id or not client_secret:
        return jsonify({'ok': False}), 400
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    lines = []
    if os.path.exists(env_path):
        with open(env_path) as f:
            lines = [l for l in f.readlines()
                     if not l.startswith('GOOGLE_CLIENT_ID=') and not l.startswith('GOOGLE_CLIENT_SECRET=')]
    lines += [f'GOOGLE_CLIENT_ID={client_id}\n', f'GOOGLE_CLIENT_SECRET={client_secret}\n']
    with open(env_path, 'w') as f:
        f.writelines(lines)
    os.environ['GOOGLE_CLIENT_ID'] = client_id
    os.environ['GOOGLE_CLIENT_SECRET'] = client_secret
    return jsonify({'ok': True})


@app.route('/api/calendar/status')
def calendar_status():
    connected = _gcal_token() is not None
    has_creds = bool(os.getenv('GOOGLE_CLIENT_ID'))
    return jsonify({'connected': connected, 'has_creds': has_creds})


@app.route('/auth/google')
def auth_google():
    client_id = os.getenv('GOOGLE_CLIENT_ID', '')
    if not client_id:
        return 'GOOGLE_CLIENT_ID not set', 400
    params = urllib.parse.urlencode({
        'client_id': client_id, 'redirect_uri': GCAL_REDIRECT,
        'response_type': 'code', 'scope': GCAL_SCOPE,
        'access_type': 'offline', 'prompt': 'consent'
    })
    return redirect(f'{GCAL_AUTH_URL}?{params}')


@app.route('/auth/google/callback')
def auth_google_callback():
    code = request.args.get('code')
    if not code:
        return redirect('/?cal_error=no_code')
    client_id = os.getenv('GOOGLE_CLIENT_ID', '')
    client_secret = os.getenv('GOOGLE_CLIENT_SECRET', '')
    data = urllib.parse.urlencode({
        'client_id': client_id, 'client_secret': client_secret,
        'code': code, 'redirect_uri': GCAL_REDIRECT,
        'grant_type': 'authorization_code'
    }).encode()
    try:
        req = urllib.request.Request(GCAL_TOKEN_URL, data=data, method='POST')
        with urllib.request.urlopen(req) as r:
            token = json.loads(r.read())
        if 'access_token' not in token:
            return redirect('/?cal_error=token_failed')
        _save_gcal_token(token)
    except Exception as e:
        return redirect(f'/?cal_error={urllib.parse.quote(str(e))}')
    return redirect('/?cal_connected=1')


@app.route('/api/calendar/create-event', methods=['POST'])
def create_calendar_event():
    token = _gcal_token()
    if not token:
        return jsonify({'error': 'Not connected'}), 401
    d = request.json
    date = d.get('date', '')
    if not date:
        return jsonify({'error': 'No date'}), 400
    # next day for all-day end
    from datetime import date as dt_date, timedelta
    end_date = (dt_date.fromisoformat(date) + timedelta(days=1)).isoformat()
    desc_parts = [p for p in [d.get('notes',''), d.get('time_estimate','') and f"Est: {d['time_estimate']}"] if p]
    event = {
        'summary': d.get('title', ''),
        'description': '\n'.join(desc_parts),
        'start': {'date': date},
        'end':   {'date': end_date},
    }
    url = f'{GCAL_API_URL}/calendars/primary/events'
    try:
        result, status = _gcal_post(url, event, token['access_token'])
        return jsonify({'ok': True, 'event_link': result.get('htmlLink', '')})
    except urllib.error.HTTPError as e:
        if e.code == 401:
            try:
                token = _refresh_gcal_token(token)
                result, _ = _gcal_post(url, event, token['access_token'])
                return jsonify({'ok': True, 'event_link': result.get('htmlLink', '')})
            except Exception as ex:
                return jsonify({'error': str(ex)}), 401
        return jsonify({'error': str(e)}), e.code


@app.route('/api/calendar/push-task/<int:tid>', methods=['POST'])
def push_task_to_gcal(tid):
    token = _gcal_token()
    if not token:
        return jsonify({'ok': False, 'error': 'Not connected'})
    d = request.json or {}
    tz = d.get('timezone', 'America/New_York')
    conn = get_db()
    task = conn.execute('SELECT * FROM tasks WHERE id=?', (tid,)).fetchone()
    conn.close()
    if not task:
        return jsonify({'ok': False, 'error': 'Not found'})
    task = dict(task)
    if not task.get('scheduled_date') or not task.get('start_time'):
        return jsonify({'ok': False, 'error': 'No date/time'})
    from datetime import datetime as dt_time, timedelta
    start_dt = dt_time.fromisoformat(f"{task['scheduled_date']}T{task['start_time']}:00")
    end_dt = start_dt + timedelta(minutes=task.get('duration_minutes') or 60)
    event = {
        'summary': task['title'],
        'description': task.get('notes', '') or '',
        'start': {'dateTime': start_dt.isoformat(), 'timeZone': tz},
        'end':   {'dateTime': end_dt.isoformat(),   'timeZone': tz},
    }
    existing_id = task.get('gcal_event_id', '')
    base_url = f'{GCAL_API_URL}/calendars/primary/events'

    def _do(tok):
        if existing_id:
            try:
                return _gcal_put(f'{base_url}/{existing_id}', event, tok['access_token'])
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return _gcal_post(base_url, event, tok['access_token'])
                raise
        return _gcal_post(base_url, event, tok['access_token'])

    try:
        result, _ = _do(token)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            try:
                token = _refresh_gcal_token(token)
                result, _ = _do(token)
            except Exception as ex:
                return jsonify({'ok': False, 'error': str(ex)})
        else:
            return jsonify({'ok': False, 'error': str(e)})
    event_id = result.get('id', '')
    conn = get_db()
    conn.execute('UPDATE tasks SET gcal_event_id=? WHERE id=?', (event_id, tid))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'event_id': event_id})


@app.route('/api/calendar/push-task/<int:tid>', methods=['DELETE'])
def delete_task_from_gcal(tid):
    token = _gcal_token()
    conn = get_db()
    task = conn.execute('SELECT gcal_event_id FROM tasks WHERE id=?', (tid,)).fetchone()
    conn.close()
    event_id = dict(task).get('gcal_event_id', '') if task else ''
    if token and event_id:
        try:
            req = urllib.request.Request(
                f'{GCAL_API_URL}/calendars/primary/events/{event_id}', method='DELETE')
            req.add_header('Authorization', f'Bearer {token["access_token"]}')
            urllib.request.urlopen(req)
        except Exception:
            pass
    conn = get_db()
    conn.execute('UPDATE tasks SET gcal_event_id="" WHERE id=?', (tid,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/api/calendar/gcal-events')
def get_gcal_events():
    token = _gcal_token()
    if not token:
        return jsonify({'events': []})
    week_start = request.args.get('week_start', '')
    if not week_start:
        return jsonify({'events': []})
    try:
        from datetime import date as dt_date, timedelta, datetime as dt_time
        start_date = dt_date.fromisoformat(week_start)
        end_date = start_date + timedelta(days=7)
        params = urllib.parse.urlencode({
            'timeMin': f'{start_date.isoformat()}T00:00:00Z',
            'timeMax': f'{end_date.isoformat()}T00:00:00Z',
            'singleEvents': 'true', 'orderBy': 'startTime', 'maxResults': '100'
        })
        url = f'{GCAL_API_URL}/calendars/primary/events?{params}'

        def _fetch(tok):
            req = urllib.request.Request(url)
            req.add_header('Authorization', f'Bearer {tok["access_token"]}')
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())

        try:
            data = _fetch(token)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                token = _refresh_gcal_token(token)
                data = _fetch(token)
            else:
                raise

        events = []
        for item in data.get('items', []):
            start = item.get('start', {})
            end_ev = item.get('end', {})
            if 'dateTime' not in start:
                continue  # skip all-day events
            start_str = start['dateTime']
            end_str = end_ev.get('dateTime', '')
            day = start_str[:10]
            start_time = start_str[11:16]
            try:
                s = dt_time.fromisoformat(start_str[:19])
                en = dt_time.fromisoformat(end_str[:19])
                duration = max(15, int(abs((en - s).total_seconds()) / 60))
            except Exception:
                duration = 60
            events.append({
                'title': item.get('summary', '(No title)'),
                'day': day, 'start_time': start_time, 'duration_minutes': duration
            })
        return jsonify({'events': events})
    except Exception as ex:
        return jsonify({'events': [], 'error': str(ex)})


@app.route('/api/calendar/disconnect', methods=['POST'])
def calendar_disconnect():
    if os.path.exists(GCAL_TOKEN_PATH):
        os.remove(GCAL_TOKEN_PATH)
    return jsonify({'ok': True})


@app.route('/api/archive/week', methods=['POST'])
def archive_week():
    from datetime import date, timedelta
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    week_label = monday.isoformat()
    conn = get_db()
    result = conn.execute(
        '''UPDATE tasks SET archived_week = ?
           WHERE task_type IN ('main','smaller','top3')
           AND (archived_week IS NULL OR archived_week = '')''',
        (week_label,)
    )
    conn.commit()
    count = result.rowcount
    conn.close()
    return jsonify({'ok': True, 'archived': count, 'week': week_label})


@app.route('/api/archive/weeks', methods=['GET'])
def get_archive_weeks():
    conn = get_db()
    weeks = conn.execute(
        '''SELECT DISTINCT archived_week FROM tasks
           WHERE archived_week IS NOT NULL AND archived_week != ""
           ORDER BY archived_week DESC'''
    ).fetchall()
    result = []
    for w in weeks:
        wk = w['archived_week']
        tasks = conn.execute(
            'SELECT * FROM tasks WHERE archived_week = ? ORDER BY completed, task_type, sort_order',
            (wk,)
        ).fetchall()
        result.append({'week': wk, 'tasks': [dict(t) for t in tasks]})
    conn.close()
    return jsonify({'weeks': result})


@app.route('/api/tasks/<int:tid>/restore', methods=['POST'])
def restore_task(tid):
    conn = get_db()
    conn.execute('UPDATE tasks SET archived_week = NULL WHERE id = ?', (tid,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/sw.js')
def service_worker():
    return send_from_directory(os.path.join(_HERE, 'static'), 'sw.js',
                               mimetype='application/javascript')


if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 8000))
    print(f'\n  The List is running → http://localhost:{port}\n')
    app.run(host='0.0.0.0', port=port, debug=False)
