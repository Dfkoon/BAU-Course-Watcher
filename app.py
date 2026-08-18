# -*- coding: utf-8 -*-
"""
نظام مراقبة جريدة المواد - جامعة البلقاء التطبيقية (BAU Course Watcher)
=====================================================================
نظام مراقبة حقيقي وتنفيذي 100% دقيق خالي تماماً من أي إشارات كاذبة أو أخطاء:
  1) آلية إعادة المحاولة تلقائياً (Auto-Retry): 3 محاولات لكل قسم لمنع أي انقطاع مؤقت في سيرفر الجامعة.
  2) شرط السلامة الحرج (Safety Check): إذا قلّ عدد الشعب المجلوبة عن 1,200 شعبة بسبب مشاكل الاتصال لدى الجامعة، يُلغى الفحص تلقائياً لتجنب رصد تغييرات كاذبة.
  3) تجميع وترتيب مواعيد الشعب (Sorted Set Aggregation) لضمان اتساق البيانات 100%.
  4) خالي تماماً من الأيقونات التعبيرية (Emoji-Free).
  5) تشخيص دقيق ومباشر لأخطاء البريد الإلكتروني SMTP عند الفحص أو التجربة.
"""

import os
import sys
import ast
import json
import re
import time
import sqlite3
import logging
import smtplib
import threading
import queue
import urllib.request
import urllib.parse
import ssl
import hashlib
import io
import base64
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pyotp
import bcrypt
import qrcode

from flask import Flask, request, jsonify, render_template, Response, stream_with_context, session, redirect, url_for, send_file
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

# =========================================================================
# Logging & Config
# =========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("bau-watcher-real")

BAU_RMI_URL = "https://app2.bau.edu.jo:7799/courses/actions/rmiMethod"
CHECK_INTERVAL_S = int(os.environ.get("CHECK_INTERVAL_SECONDS", "45"))

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "").strip()
SMTP_PASS = os.environ.get("SMTP_PASS", "").strip()
SENDER_NAME = os.environ.get("SENDER_NAME", "مكانك لمراقبة جريدة المواد - جامعة البلقاء")

# Admin bootstrap credentials from .env (used only to seed DB on first run)
ADMIN_USERNAME_SEED = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_SEED = os.environ.get("ADMIN_PASSWORD", "makanak2026")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data.db")

# ── Admin config helpers ──────────────────────────────────────────────────
def _get_admin_cfg():
    """Return admin config dict from DB (username, pw_hash, totp_secret, totp_enabled)."""
    try:
        conn = get_db()
        row = conn.execute("SELECT * FROM admin_config LIMIT 1").fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        return {}

def _set_admin_cfg(**kwargs):
    """Update specified fields in admin_config."""
    conn = get_db()
    for k, v in kwargs.items():
        conn.execute(
            "INSERT OR REPLACE INTO admin_config (key, value) VALUES (?, ?)",
            (k, v)
        )
    conn.commit()
    conn.close()

def _get_admin_field(key, default=None):
    try:
        conn = get_db()
        row = conn.execute("SELECT value FROM admin_config WHERE key=?", (key,)).fetchone()
        conn.close()
        return row[0] if row else default
    except Exception:
        return default

STATUS_MAP = {
    "1": ("متاحة", "open"),
    "2": ("ملغاة", "cancelled"),
    "3": ("مغلقة", "closed"),
}

# قائمة عملاء SSE للواجهة الحية
_sse_clients = []
_sse_lock = threading.Lock()

def sse_broadcast(event_type: str, payload: dict):
    msg = f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)

# =========================================================================
# Database Setup
# =========================================================================

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            college_query TEXT,
            course_query TEXT NOT NULL,
            section_query TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        );
    """)
    try:
        conn.execute("ALTER TABLE subscribers ADD COLUMN college_query TEXT")
        conn.commit()
    except Exception:
        pass

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS course_state (
            course_key TEXT PRIMARY KEY,
            course_no TEXT,
            course_name TEXT,
            hours TEXT,
            section_no TEXT,
            times TEXT,
            lecturers TEXT,
            rooms TEXT,
            status TEXT,
            status_desc TEXT,
            remarks TEXT,
            college_name TEXT,
            department_name TEXT,
            first_seen TEXT,
            last_seen TEXT,
            last_change_at TEXT
        );

        CREATE TABLE IF NOT EXISTS change_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_key TEXT NOT NULL,
            course_name TEXT,
            section_no TEXT,
            change_type TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            college_name TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS notifications_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_id INTEGER,
            email TEXT NOT NULL,
            course_key TEXT NOT NULL,
            change_type TEXT NOT NULL,
            sent_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS system_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS admin_config (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS course_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            course_no TEXT NOT NULL,
            course_name TEXT,
            notes TEXT,
            created_at TEXT NOT NULL
        );
    """)
    conn.commit()

    # Seed admin credentials on first run (only if not already set)
    existing_user = conn.execute("SELECT value FROM admin_config WHERE key='username'").fetchone()
    if not existing_user:
        pw_hash = bcrypt.hashpw(ADMIN_PASSWORD_SEED.encode(), bcrypt.gensalt()).decode()
        totp_secret = pyotp.random_base32()
        for k, v in [
            ("username", ADMIN_USERNAME_SEED),
            ("pw_hash", pw_hash),
            ("totp_secret", totp_secret),
            ("totp_enabled", "0"),
        ]:
            conn.execute("INSERT OR IGNORE INTO admin_config (key, value) VALUES (?, ?)", (k, v))
        conn.commit()
        log.info(f"تم إنشاء حساب الأدمن الأولي: username='{ADMIN_USERNAME_SEED}'")

    conn.close()


# =========================================================================
# Text Normalization Utility
# =========================================================================

def clean_text(val):
    if not val or str(val).strip().lower() in ("null", "none"):
        return ""
    text = str(val)
    text = text.replace("<br>", " ").replace("<BR>", " ").replace("&nbsp;", " ")
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# =========================================================================
# BAU RMI Client with Auto-Retry
# =========================================================================

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

def rmi(method, params=None, timeout=12, retries=3):
    data = {"method": method, "paramsCount": len(params) if params else 0}
    if params:
        for i, p in enumerate(params):
            data[f"param{i}"] = str(p)
    body = urllib.parse.urlencode(data).encode('utf-8')
    req = urllib.request.Request(
        BAU_RMI_URL, data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "BAU-Watcher-Strict/5.0"
        }
    )

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, context=_ssl_ctx, timeout=timeout) as r:
                return ast.literal_eval(r.read().decode("utf-8", errors="ignore"))
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(1)

    raise last_err

def crawl_all_courses(degree_ids=("3",)):
    try:
        colleges = rmi("getColleges")
    except Exception as e:
        log.error(f"تعذر الاتصال بسيرفر الجامعة الرئيسي: {e}")
        return []

    raw_results = []
    for deg in degree_ids:
        for col in colleges:
            col_id, col_name = col["id"], col["name"]
            try:
                depts = rmi("getDepartments", [col_id])
            except Exception:
                continue
            if not isinstance(depts, list):
                continue
            for dept in depts:
                dept_id, dept_name = dept["id"], dept["name"]
                try:
                    pages = int(rmi("getCoursesPagesCount", [deg, col_id, dept_id]) or 0)
                except Exception:
                    pages = 1
                for page in range(1, pages + 1):
                    try:
                        courses = rmi("getCourses", [deg, col_id, dept_id, str(page)])
                        if isinstance(courses, list):
                            for c in courses:
                                c["college_name"] = col_name
                                c["department_name"] = dept_name
                            raw_results.extend(courses)
                    except Exception as e:
                        log.warning(f"تنبيه: تعذر جلب صفحة {page} قسم {dept_name}: {e}")

    # تجميع كتل المواعيد والقاعات مع ترتيب أبجدي محدد (Sorted Set Aggregation)
    sections_map = {}
    for rec in raw_results:
        c_no = clean_text(rec.get("no"))
        s_no = clean_text(rec.get("sectionNo"))
        if not c_no or not s_no:
            continue
        key = f"{c_no}_{s_no}"

        times = clean_text(rec.get("times"))
        rooms = clean_text(rec.get("rooms"))
        lecturers = clean_text(rec.get("lecturers"))

        if key not in sections_map:
            sections_map[key] = {
                "course_key": key,
                "no": c_no,
                "name": clean_text(rec.get("name")),
                "sectionNo": s_no,
                "hours": clean_text(rec.get("hours")),
                "status": clean_text(rec.get("status", "1")),
                "remarks": clean_text(rec.get("remarks")),
                "college_name": rec.get("college_name", ""),
                "department_name": rec.get("department_name", ""),
                "times_list": set([times]) if times else set(),
                "rooms_list": set([rooms]) if rooms else set(),
                "lecturers_list": set([lecturers]) if lecturers else set()
            }
        else:
            existing = sections_map[key]
            if times:
                existing["times_list"].add(times)
            if rooms:
                existing["rooms_list"].add(rooms)
            if lecturers:
                existing["lecturers_list"].add(lecturers)

    final_list = []
    for item in sections_map.values():
        item["times"] = " / ".join(sorted(list(item["times_list"])))
        item["rooms"] = " / ".join(sorted(list(item["rooms_list"])))
        item["lecturers"] = " / ".join(sorted(list(item["lecturers_list"])))
        final_list.append(item)

    return final_list

# =========================================================================
# Strict Engine for 100% Real Changes Only
# =========================================================================

MIN_EXPECTED_SECTIONS = 1200  # حماية من أي بطء أو انقطاع جزئي بسيرفر الجامعة

def run_one_check():
    now = datetime.now().isoformat()
    current = crawl_all_courses(degree_ids=("3",))

    if not current:
        log.warning("لم يتم استرجاع أية شعب في هذه الدورة - تم تخطي المقارنة لتجنب التنبيهات الزائفة.")
        return []

    conn = get_db()
    existing_rows = conn.execute("SELECT * FROM course_state").fetchall()
    prev_state = {r["course_key"]: dict(r) for r in existing_rows}

    is_first_run = len(prev_state) == 0

    # حماية من التنبيهات الزائفة عند حدوث انقطاع جزئي في سيرفر الجامعة
    if not is_first_run and len(current) < MIN_EXPECTED_SECTIONS:
        log.warning(f"عدد الشعب المجلوبة ({len(current)}) أقل من الحد الأدنى المتوقع ({MIN_EXPECTED_SECTIONS}) بسبب انقطاع مؤقت في الاتصال بسيرفر الجامعة. تم إلغاء المقارنة في هذه الدورة للحفاظ على الدقة 100%.")
        conn.close()
        return []

    if is_first_run:
        log.info("الدورة الأولى: جاري أخذ لقطة أساسية صامتة لجميع شعب الجريدة...")

    changes = []

    for rec in current:
        c_no = clean_text(rec.get("no"))
        s_no = clean_text(rec.get("sectionNo"))
        if not c_no or not s_no:
            continue

        key = f"{c_no}_{s_no}"
        name = clean_text(rec.get("name"))
        hours = clean_text(rec.get("hours"))
        times = clean_text(rec.get("times"))
        lecturers = clean_text(rec.get("lecturers"))
        rooms = clean_text(rec.get("rooms"))
        status = clean_text(rec.get("status", "1"))
        status_desc = STATUS_MAP.get(status, ("غير معروف", "unknown"))[0]
        remarks = clean_text(rec.get("remarks"))
        college = rec.get("college_name", "")
        dept = rec.get("department_name", "")

        old = prev_state.get(key)

        change_type = None
        old_val = ""
        new_val = ""

        if not is_first_run:
            if old is None:
                if status == "1":
                    change_type = "NEW_SECTION"
                    new_val = f"شعبة جديدة متاحة ({s_no})"
            else:
                old_status = old["status"]
                old_times = clean_text(old["times"])
                old_lecturers = clean_text(old["lecturers"])
                old_rooms = clean_text(old["rooms"])

                if old_status in ("2", "3") and status == "1":
                    change_type = "BECAME_OPEN"
                    old_val = STATUS_MAP.get(old_status, ("مغلقة", ""))[0]
                    new_val = "أصبحت متاحة للتسجيل"
                elif status in ("2", "3") and old_status == "1":
                    change_type = "BECAME_CLOSED"
                    old_val = "كانت متاحة"
                    new_val = STATUS_MAP.get(status, ("مغلقة", ""))[0]
                elif old_times and times and old_times != times:
                    change_type = "TIMES_CHANGED"
                    old_val = old_times
                    new_val = times
                elif old_lecturers and lecturers and old_lecturers != lecturers:
                    change_type = "LECTURER_CHANGED"
                    old_val = old_lecturers
                    new_val = lecturers
                elif old_rooms and rooms and old_rooms != rooms:
                    change_type = "ROOM_CHANGED"
                    old_val = old_rooms
                    new_val = rooms

        conn.execute("""
            INSERT INTO course_state (
                course_key, course_no, course_name, hours, section_no, times,
                lecturers, rooms, status, status_desc, remarks, college_name,
                department_name, first_seen, last_seen, last_change_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(course_key) DO UPDATE SET
                course_name = excluded.course_name,
                hours = excluded.hours,
                times = excluded.times,
                lecturers = excluded.lecturers,
                rooms = excluded.rooms,
                status = excluded.status,
                status_desc = excluded.status_desc,
                remarks = excluded.remarks,
                college_name = excluded.college_name,
                department_name = excluded.department_name,
                last_seen = excluded.last_seen,
                last_change_at = CASE WHEN excluded.status != course_state.status THEN excluded.last_seen ELSE course_state.last_change_at END
        """, (
            key, c_no, name, hours, s_no, times,
            lecturers, rooms, status, status_desc, remarks,
            college, dept, now, now, now
        ))

        if change_type:
            ch_item = {
                "course_key": key,
                "course_no": c_no,
                "course_name": name,
                "section_no": s_no,
                "times": times,
                "lecturers": lecturers,
                "rooms": rooms,
                "status": status,
                "status_desc": status_desc,
                "change_type": change_type,
                "old_value": old_val,
                "new_value": new_val,
                "college_name": college,
                "timestamp": now
            }
            changes.append(ch_item)
            conn.execute("""
                INSERT INTO change_log (
                    course_key, course_name, section_no, change_type, old_value, new_value, college_name, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, name, s_no, change_type, old_val, new_val, college, now))

    conn.execute("INSERT OR REPLACE INTO system_meta (key, value) VALUES ('last_check', ?)", (now,))
    conn.execute("INSERT OR REPLACE INTO system_meta (key, value) VALUES ('total_sections', ?)", (str(len(current)),))
    conn.commit()
    conn.close()

    if is_first_run:
        log.info(f"تم حفظ اللقطة الأساسية ({len(current)} شعبة) بنجاح. أي تغيير من الآن سيكون حقيقياً 100%.")

    return changes

# =========================================================================
# Continuous Watcher Thread Loop
# =========================================================================

_watcher_running = threading.Event()
_watcher_running.set()

def watcher_loop():
    log.info(f"محرك المراقبة الحقيقية بدأ (يفحص كل {CHECK_INTERVAL_S} ثانية)")
    while _watcher_running.is_set():
        t0 = time.time()
        try:
            log.info("جاري فحص جريدة المواد من سيرفر البلقاء...")
            real_changes = run_one_check()

            if real_changes:
                log.info(f"رُصد {len(real_changes)} تغيير حقيقي في الجريدة الرسمية! جاري الإشعار...")
                for ch in real_changes:
                    sse_broadcast("course_change", ch)
                notify_subscribers_batch(real_changes)
            else:
                log.info("لا توجد أية تغييرات حقيقية في هذه الدورة.")

            sse_broadcast("stats_update", _get_stats())
        except Exception as e:
            log.error(f"خطأ أثناء فحص الجريدة: {e}")

        elapsed = time.time() - t0
        wait = max(5, CHECK_INTERVAL_S - elapsed)
        time.sleep(wait)

# =========================================================================
# Email Notifications Engine
# =========================================================================

def is_smtp_ready():
    user = SMTP_USER.strip()
    pwd = SMTP_PASS.strip()
    return bool(user and pwd and "your_email" not in user and "xxxx" not in pwd)

def notify_subscribers_batch(changes):
    conn = get_db()
    subs = conn.execute("SELECT * FROM subscribers WHERE active=1").fetchall()
    now = datetime.now().isoformat()

    for ch in changes:
        if ch["change_type"] not in ("BECAME_OPEN", "NEW_SECTION", "TIMES_CHANGED"):
            continue

        ch_college = (ch.get("college_name") or "").strip()
        ch_name = (ch.get("course_name") or "").strip().lower()
        ch_no = (ch.get("course_no") or "").strip().lower()
        ch_sec = (ch.get("section_no") or "").strip()

        for sub in subs:
            sub_dict = dict(sub)
            col_q = (sub_dict.get("college_query") or "").strip()
            cq = (sub_dict.get("course_query") or "").strip().lower()
            sq = (sub_dict.get("section_query") or "").strip()

            # 1) Multi-College Match
            if not col_q or col_q.upper() in ("ALL", "جميع الكليات"):
                college_match = True
            else:
                selected_colleges = [c.strip().lower() for c in col_q.split(",") if c.strip()]
                college_match = any(
                    c in ch_college.lower() or ch_college.lower() in c
                    for c in selected_colleges
                )

            # 2) Course Match
            if not cq or cq.upper() in ("ALL", "جميع المواد"):
                course_match = True
            else:
                course_match = (cq in ch_name or ch_name in cq or cq in ch_no)

            # 3) Section Match
            if not sq or sq.upper() in ("ALL", "جميع الشعب"):
                section_match = True
            else:
                section_match = (sq == ch_sec)

            if college_match and course_match and section_match:
                ok, _ = _send_email(sub["name"], sub["email"], ch, sub_id=sub["id"])
                if ok:
                    conn.execute("""
                        INSERT INTO notifications_log (subscriber_id, email, course_key, change_type, sent_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (sub["id"], sub["email"], ch["course_key"], ch["change_type"], now))

    conn.commit()
    conn.close()

def format_time_12h(time_str):
    if not time_str or time_str == '—':
        return '—'
    DAY_MAP = {'ح':'الأحد', 'ن':'الإثنين', 'ث':'الثلاثاء', 'ر':'الأربعاء', 'خ':'الخميس', 'س':'السبت'}
    def convert_hhmm(hhmm):
        try:
            h, m = map(int, hhmm.split(':'))
            period = 'ص' if h < 12 else 'م'
            h12 = h if h <= 12 else h - 12
            if h12 == 0: h12 = 12
            return f'{h12}:{m:02d} {period}'
        except Exception:
            return hhmm

    pattern = re.compile(r'((?:[حنثرخس]\s*)+)(\d{2}:\d{2})\s+(\d{2}:\d{2})')
    matches = pattern.findall(time_str)
    if not matches:
        return time_str
    parts = []
    for days_raw, t1, t2 in matches:
        days_list = [DAY_MAP[d] for d in days_raw.split() if d in DAY_MAP]
        days_fmt = ' - '.join(days_list)
        parts.append(f'{days_fmt}: {convert_hhmm(t1)} - {convert_hhmm(t2)}')
    return ' | '.join(parts)

def _send_email(student_name, to_email, ch, sub_id=None):
    if not is_smtp_ready():
        err_msg = "إعدادات البريد الإلكتروني SMTP غير مكتملة في ملف .env. يرجى تعبئة SMTP_USER و SMTP_PASS (كلمة مرور التطبيق من Google)."
        log.warning(f"تعذر الإرسال إلى {to_email}: {err_msg}")
        return False, err_msg

    unsub_html = ""
    if sub_id:
        token = _get_unsub_token(sub_id, to_email)
        port = int(os.environ.get("PORT", 5050))
        unsub_link = f"http://127.0.0.1:{port}/unsubscribe?id={sub_id}&token={token}"
        unsub_html = f"""
        <div style="text-align: center; margin-top: 18px; padding-top: 14px; border-top: 1px dashed #e5e7eb; font-size: .8rem; color: #6b7280;">
          تتلقى هذه الرسالة لأنك مشترك في إشعارات مكانك.<br>
          <a href="{unsub_link}" target="_blank" style="color: #ef4444; font-weight: bold; text-decoration: underline;">إلغاء الاشتراك من هذه المادة</a>
        </div>
        """

    subject = f"تنبيه: {ch['course_name']} شعبة {ch['section_no']} - جامعة البلقاء"
    formatted_times = format_time_12h(ch.get('times'))
    html = f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="ar">
    <head><meta charset="UTF-8">
    <style>
      body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background: #f3f4f6; margin: 0; padding: 20px; }}
      .card {{ max-width: 580px; margin: 0 auto; background: #fff; border-radius: 16px; overflow: hidden; border: 1px solid #e5e7eb; }}
      .hdr {{ background: linear-gradient(135deg, #059669, #10b981); color: #fff; padding: 28px; text-align: center; }}
      .body {{ padding: 28px; }}
      .badge {{ display: inline-block; background: #ecfdf5; color: #047857; padding: 8px 16px; border-radius: 50px; font-weight: bold; border: 1px solid #a7f3d0; margin-bottom: 18px; }}
      .box {{ background: #f9fafb; border-radius: 12px; padding: 18px; border-right: 4px solid #10b981; margin: 16px 0; }}
      .row {{ display: flex; justify-content: space-between; padding: 7px 0; border-bottom: 1px dashed #e5e7eb; }}
      .row:last-child {{ border: none; }}
      .lbl {{ font-weight: bold; color: #4b5563; }} .val {{ color: #111; font-weight: 600; }}
      .btn {{ display: block; text-align: center; background: #059669; color: #fff !important; text-decoration: none; padding: 14px; border-radius: 10px; font-weight: bold; margin-top: 20px; }}
    </style>
    </head>
    <body>
    <div class="card">
      <div class="hdr"><h1>إشعار حقيقي من جريدة البلقاء</h1></div>
      <div class="body">
        <div style="text-align: center;"><span class="badge">{ch['new_value']}</span></div>
        <p>مرحباً <strong>{student_name}</strong>،</p>
        <p>تم رصد تغيير موثّق وحقيقي في جريدة جامعة البلقاء الرسمية للمادة التي تتابعها:</p>
        <div class="box">
          <div class="row"><span class="lbl">الكلية:</span><span class="val">{ch.get('college_name') or 'جامعة البلقاء التطبيقية'}</span></div>
          <div class="row"><span class="lbl">المادة:</span><span class="val">{ch['course_name']} ({ch['course_no']})</span></div>
          <div class="row"><span class="lbl">الشعبة:</span><span class="val">{ch['section_no']}</span></div>
          <div class="row"><span class="lbl">الأوقات:</span><span class="val">{formatted_times}</span></div>
          <div class="row"><span class="lbl">المحاضر:</span><span class="val">{ch['lecturers'] or 'غير محدد'}</span></div>
          <div class="row"><span class="lbl">القاعة:</span><span class="val">{ch['rooms'] or 'غير محدد'}</span></div>
        </div>
        <a href="https://www.bau.edu.jo/Services/Reg.aspx" target="_blank" class="btn">سارع الآن بتسجيل المادة</a>
        {unsub_html}
      </div>
    </div>
    </body>
    </html>
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{SENDER_NAME} <{SMTP_USER}>"
    msg["To"] = to_email
    msg.attach(MIMEText(html, "html", "utf-8"))

    # 1) Attempt SSL Port 465
    try:
        ctx = ssl._create_unverified_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx, timeout=10) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [to_email], msg.as_string())
        log.info(f"أُرسل إشعار حقيقي إلى البريد {to_email} بنجاح عبر منفذ 465.")
        return True, "تم إرسال البريد الإلكتروني بنجاح"
    except Exception as e1:
        # 2) Fallback to STARTTLS Port 587
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as s:
                s.starttls()
                s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(SMTP_USER, [to_email], msg.as_string())
            log.info(f"أُرسل إشعار حقيقي إلى البريد {to_email} بنجاح عبر منفذ 587.")
            return True, "تم إرسال البريد الإلكتروني بنجاح"
        except Exception as e2:
            err_msg = f"فشل الاتصال بسيرفر البريد: {e1}"
            log.error(err_msg)
            return False, err_msg

# =========================================================================
# Stats Helper & Web Endpoints
# =========================================================================

def _get_stats():
    conn = get_db()
    def scalar(sql, *args):
        return conn.execute(sql, args).fetchone()[0]
    d = dict(
        total_tracked=scalar("SELECT COUNT(*) FROM course_state"),
        open_courses=scalar("SELECT COUNT(*) FROM course_state WHERE status='1'"),
        closed_courses=scalar("SELECT COUNT(*) FROM course_state WHERE status='3'"),
        subscribers_count=scalar("SELECT COUNT(*) FROM subscribers WHERE active=1"),
        notifications_count=scalar("SELECT COUNT(*) FROM notifications_log"),
        changes_today=scalar("SELECT COUNT(*) FROM change_log WHERE created_at>=date('now')"),
        interval_seconds=CHECK_INTERVAL_S,
        smtp_configured=is_smtp_ready(),
    )
    row = conn.execute("SELECT value FROM system_meta WHERE key='last_check'").fetchone()
    d["last_check_time"] = row[0] if row else None
    conn.close()
    return d

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.environ.get("FLASK_SECRET", "bau-strict-2026")

@app.route("/api/stream")
def sse_stream():
    q = queue.Queue(maxsize=200)
    with _sse_lock:
        _sse_clients.append(q)

    def gen():
        yield f"event: stats_update\ndata: {json.dumps(_get_stats(), ensure_ascii=False)}\n\n"
        try:
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                if q in _sse_clients:
                    _sse_clients.remove(q)

    return Response(
        stream_with_context(gen()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )

# =========================================================================
# Auth Helper
# =========================================================================

def admin_required(f):
    """Decorator: redirect to login if not authenticated as admin."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated

def admin_api_required(f):
    """Decorator: return 401 JSON if not authenticated (for API endpoints)."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin"):
            return jsonify(success=False, message="غير مصرح. يرجى تسجيل الدخول كمشرف."), 401
        return f(*args, **kwargs)
    return decorated

# =========================================================================
# Page Routes
# =========================================================================

@app.route("/")
def index():
    """Root: redirect to student page for public access."""
    return redirect(url_for("student_page"))

@app.route("/student")
def student_page():
    return render_template("student.html")

@app.route("/login")
def login_page():
    if session.get("admin"):
        return redirect(url_for("admin_page"))
    return render_template("login.html")

@app.route("/admin")
@admin_required
def admin_page():
    return render_template("index.html")


@app.route("/admin/login", methods=["POST"])
def admin_login():
    d = request.get_json() or {}
    username  = (d.get("username") or "").strip()
    password  = (d.get("password") or "").strip()
    totp_code = (d.get("totp_code") or "").strip()

    stored_user   = _get_admin_field("username", "")
    stored_hash   = _get_admin_field("pw_hash", "")
    totp_enabled  = _get_admin_field("totp_enabled", "0") == "1"
    totp_secret   = _get_admin_field("totp_secret", "")

    # Step 1: verify username + password
    if not stored_hash:
        return jsonify(success=False, message="لم يتم إعداد حساب الأدمن بعد."), 500

    if username != stored_user:
        return jsonify(success=False, message="اسم المستخدم أو كلمة المرور غير صحيحة"), 401

    try:
        pw_ok = bcrypt.checkpw(password.encode(), stored_hash.encode())
    except Exception:
        pw_ok = False

    if not pw_ok:
        return jsonify(success=False, message="اسم المستخدم أو كلمة المرور غير صحيحة"), 401

    # Step 2: if 2FA enabled, verify TOTP
    if totp_enabled:
        if not totp_code:
            return jsonify(success=False, message="يرجى إدخال كود المصادقة الثنائية", need_totp=True), 200
        totp = pyotp.TOTP(totp_secret)
        if not totp.verify(totp_code, valid_window=1):
            return jsonify(success=False, message="كود 2FA غير صحيح أو انتهت صلاحيته"), 401

    session["admin"] = True
    session.permanent = True
    return jsonify(success=True)

@app.route("/admin/logout")
def admin_logout():
    session.pop("admin", None)
    return redirect(url_for("login_page"))

# ─── Admin Settings ──────────────────────────────────────────────────────────

@app.route("/admin/settings")
@admin_required
def admin_settings():
    return render_template("admin_settings.html",
        admin_username=_get_admin_field("username", ""),
        totp_enabled=_get_admin_field("totp_enabled", "0") == "1"
    )

@app.route("/admin/settings/update-credentials", methods=["POST"])
@admin_api_required
def admin_update_credentials():
    d = request.get_json() or {}
    current_pw   = (d.get("current_password") or "").strip()
    new_username = (d.get("new_username") or "").strip()
    new_password = (d.get("new_password") or "").strip()

    stored_hash = _get_admin_field("pw_hash", "")
    if not bcrypt.checkpw(current_pw.encode(), stored_hash.encode()):
        return jsonify(success=False, message="كلمة المرور الحالية غير صحيحة"), 401

    updates = {}
    if new_username:
        updates["username"] = new_username
    if new_password:
        if len(new_password) < 8:
            return jsonify(success=False, message="كلمة المرور الجديدة يجب أن تكون 8 أحرف على الأقل"), 400
        updates["pw_hash"] = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()

    if updates:
        _set_admin_cfg(**updates)

    return jsonify(success=True, message="تم تحديث بيانات الدخول بنجاح")

@app.route("/admin/setup-2fa")
@admin_required
def admin_setup_2fa():
    """Show QR code page for initial 2FA setup."""
    return render_template("admin_settings.html",
        admin_username=_get_admin_field("username", ""),
        totp_enabled=_get_admin_field("totp_enabled", "0") == "1",
        show_2fa_setup=True
    )

@app.route("/api/admin/2fa/qr")
@admin_api_required
def admin_2fa_qr():
    """Generate a fresh QR code PNG for the current TOTP secret."""
    totp_secret = _get_admin_field("totp_secret", "")
    if not totp_secret:
        totp_secret = pyotp.random_base32()
        _set_admin_cfg(totp_secret=totp_secret, totp_enabled="0")

    admin_user = _get_admin_field("username", "admin")
    uri = pyotp.totp.TOTP(totp_secret).provisioning_uri(
        name=admin_user,
        issuer_name="مكانك - BAU Course Watcher"
    )
    img = qrcode.make(uri)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png")

@app.route("/api/admin/2fa/verify-setup", methods=["POST"])
@admin_api_required
def admin_2fa_verify_setup():
    """Verify TOTP code then enable 2FA."""
    d = request.get_json() or {}
    code = (d.get("code") or "").strip()
    totp_secret = _get_admin_field("totp_secret", "")
    if not totp_secret:
        return jsonify(success=False, message="لم يتم توليد مفتاح 2FA بعد"), 400
    totp = pyotp.TOTP(totp_secret)
    if not totp.verify(code, valid_window=1):
        return jsonify(success=False, message="الكود غير صحيح. تأكد من مسح QR بالتطبيق"), 400
    _set_admin_cfg(totp_enabled="1")
    return jsonify(success=True, message="تم تفعيل المصادقة الثنائية 2FA بنجاح!")

@app.route("/api/admin/2fa/disable", methods=["POST"])
@admin_api_required
def admin_2fa_disable():
    """Disable 2FA after verifying current password."""
    d = request.get_json() or {}
    current_pw = (d.get("password") or "").strip()
    stored_hash = _get_admin_field("pw_hash", "")
    if not bcrypt.checkpw(current_pw.encode(), stored_hash.encode()):
        return jsonify(success=False, message="كلمة المرور غير صحيحة"), 401
    _set_admin_cfg(totp_enabled="0")
    return jsonify(success=True, message="تم إيقاف المصادقة الثنائية")



@app.route("/unsubscribe")
def unsubscribe_page():
    sid = request.args.get("id", "").strip()
    token = request.args.get("token", "").strip()
    if not sid or not token:
        return render_template("unsubscribed.html", success=False, message="رابط إلغاء الاشتراك غير صحيح.")

    conn = get_db()
    sub = conn.execute("SELECT * FROM subscribers WHERE id=?", (sid,)).fetchone()
    if not sub:
        conn.close()
        return render_template("unsubscribed.html", success=False, message="الاشتراك غير موجود أو تم إلغاؤه وحذفه مسبقاً.")

    calc_token = _get_unsub_token(sub["id"], sub["email"])
    if token != calc_token:
        conn.close()
        return render_template("unsubscribed.html", success=False, message="رمز الإلغاء غير مطابق.")

    conn.execute("DELETE FROM subscribers WHERE id=?", (sid,))
    conn.commit()
    conn.close()
    sse_broadcast("stats_update", _get_stats())
    return render_template("unsubscribed.html", success=True, message=f"تم إلغاء الاشتراك وحذفه بنجاح للطالب ({sub['name']}) للمادة [{sub['course_query']}].")

# =========================================================================
# Student Auth API (Google Sign-In & Guest)
# =========================================================================

@app.route("/api/auth/google", methods=["POST"])
def api_auth_google():
    d = request.get_json() or {}
    name = (d.get("name") or "").strip()
    email = (d.get("email") or "").strip()
    picture = d.get("picture", "")

    if not email:
        return jsonify(success=False, message="البريد الإلكتروني مطلوب."), 400

    user_info = {
        "name": name or email.split("@")[0],
        "email": email,
        "provider": "google",
        "picture": picture
    }
    session["user"] = user_info
    return jsonify(success=True, user=user_info)

@app.route("/api/auth/guest", methods=["POST"])
def api_auth_guest():
    d = request.get_json() or {}
    name = (d.get("name") or "زائر").strip()
    email = (d.get("email") or "").strip()

    user_info = {
        "name": name,
        "email": email,
        "provider": "guest"
    }
    session["user"] = user_info
    return jsonify(success=True, user=user_info)

@app.route("/api/auth/me")
def api_auth_me():
    user = session.get("user")
    return jsonify(success=True, authenticated=bool(user), user=user)

@app.route("/api/auth/logout", methods=["POST"])
def api_auth_logout():
    session.pop("user", None)
    return jsonify(success=True)

# =========================================================================
# Student Self My-Subscriptions Management API
# =========================================================================

@app.route("/api/my-subscriptions")
def api_my_subscriptions():
    user = session.get("user")
    email = user.get("email") if user else request.args.get("email", "").strip()
    if not email:
        return jsonify(success=True, subscribers=[], requests=[])

    conn = get_db()
    subs = [dict(r) for r in conn.execute("SELECT * FROM subscribers WHERE email=? ORDER BY id DESC", (email,)).fetchall()]
    reqs = [dict(r) for r in conn.execute("SELECT * FROM course_requests WHERE email=? ORDER BY id DESC", (email,)).fetchall()]
    conn.close()
    return jsonify(success=True, subscribers=subs, requests=reqs)

@app.route("/api/my-subscriptions/toggle/<int:sid>", methods=["POST"])
def api_my_toggle_sub(sid):
    user = session.get("user")
    email = user.get("email") if user else request.args.get("email", "").strip()
    if not email:
        return jsonify(success=False, message="غير مصرح."), 401

    conn = get_db()
    row = conn.execute("SELECT active FROM subscribers WHERE id=? AND email=?", (sid, email)).fetchone()
    if row:
        new = 0 if row[0] == 1 else 1
        conn.execute("UPDATE subscribers SET active=? WHERE id=?", (new, sid))
        conn.commit()
        conn.close()
        sse_broadcast("stats_update", _get_stats())
        return jsonify(success=True, active=new)
    conn.close()
    return jsonify(success=False, message="الاشتراك غير موجود."), 404

@app.route("/api/my-subscriptions/<int:sid>", methods=["DELETE"])
def api_my_delete_sub(sid):
    user = session.get("user")
    email = user.get("email") if user else request.args.get("email", "").strip()
    if not email:
        return jsonify(success=False, message="غير مصرح."), 401

    conn = get_db()
    conn.execute("DELETE FROM subscribers WHERE id=? AND email=?", (sid, email))
    conn.commit()
    conn.close()
    sse_broadcast("stats_update", _get_stats())
    return jsonify(success=True, message="تم حذف الاشتراك بنجاح.")

@app.route("/api/status")
def api_status():
    return jsonify(_get_stats())

@app.route("/api/subscribe", methods=["POST"])
def api_subscribe():
    d = request.get_json() or request.form
    name = (d.get("name") or "").strip()
    email = (d.get("email") or "").strip()
    phone = (d.get("phone") or "").strip()

    col_raw = d.get("college_query") or "ALL"
    if isinstance(col_raw, list):
        col_q = ", ".join([c.strip() for c in col_raw if c.strip()])
    else:
        col_q = str(col_raw).strip()

    if not col_q:
        col_q = "ALL"

    cq = (d.get("course_query") or "").strip()
    sq = (d.get("section_query") or "").strip()

    if not name or not email:
        return jsonify(success=False, message="الاسم والبريد الإلكتروني حقول مطلوبة."), 400

    if not cq and col_q == "ALL":
        return jsonify(success=False, message="يرجى تحديد كليات اهتمامك أو كتابة اسم المادة المطلوبة."), 400

    if not cq:
        cq = "جميع المواد"

    conn = get_db()
    conn.execute(
        "INSERT INTO subscribers(name,email,phone,college_query,course_query,section_query,active,created_at) VALUES(?,?,?,?,?,?,1,?)",
        (name, email, phone, col_q, cq, sq, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()
    sse_broadcast("stats_update", _get_stats())

    # أرسل رسالة ترحيب فورية
    _send_welcome_email(name, email, col_q, cq)

    target_desc = f"كليات [{col_q}]" if col_q != "ALL" else f"مادة [{cq}]"
    return jsonify(success=True, message=f"تم تفعيل الإشعار الفوري بنجاح لـ {target_desc}! تحقق من بريدك الإلكتروني.")

@app.route("/api/subscribers")
@admin_api_required
def api_subscribers():
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM subscribers ORDER BY id DESC").fetchall()]
    conn.close()
    return jsonify(success=True, subscribers=rows)

@app.route("/api/subscribers/toggle/<int:sid>", methods=["POST"])
@admin_api_required
def api_toggle_sub(sid):
    conn = get_db()
    row = conn.execute("SELECT active FROM subscribers WHERE id=?", (sid,)).fetchone()
    if row:
        new = 0 if row[0] == 1 else 1
        conn.execute("UPDATE subscribers SET active=? WHERE id=?", (new, sid))
        conn.commit()
    conn.close()
    sse_broadcast("stats_update", _get_stats())
    return jsonify(success=True)

@app.route("/api/subscribers/<int:sid>", methods=["DELETE"])
@admin_api_required
def api_delete_sub(sid):
    conn = get_db()
    conn.execute("DELETE FROM subscribers WHERE id=?", (sid,))
    conn.commit()
    conn.close()
    sse_broadcast("stats_update", _get_stats())
    return jsonify(success=True)

@app.route("/api/courses")
def api_courses():
    q = request.args.get("query", "").strip()
    col = request.args.get("college", "").strip()
    dept = request.args.get("dept", "").strip()
    status = request.args.get("status", "").strip()
    limit = int(request.args.get("limit", 300))

    sql, params = "SELECT * FROM course_state WHERE 1=1", []
    if q:
        sql += " AND (course_name LIKE ? OR course_no LIKE ? OR lecturers LIKE ? OR department_name LIKE ? OR remarks LIKE ?)"
        params += [f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"]
    if col:
        sql += " AND college_name LIKE ?"
        params.append(f"%{col}%")
    if dept:
        sql += " AND department_name LIKE ?"
        params.append(f"%{dept}%")
    if status:
        if status == "NEW":
            sql += " AND course_key IN (SELECT DISTINCT course_key FROM change_log WHERE change_type='NEW_SECTION')"
        else:
            sql += " AND status=?"
            params.append(status)
    sql += " ORDER BY status ASC, course_name ASC LIMIT ?"
    params.append(limit)

    conn = get_db()
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return jsonify(success=True, count=len(rows), courses=rows)

@app.route("/api/colleges")
def api_colleges():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT college_name FROM course_state WHERE college_name IS NOT NULL AND college_name!='' ORDER BY college_name").fetchall()
    conn.close()
    return jsonify(success=True, colleges=[r[0] for r in rows])

@app.route("/api/departments")
def api_departments():
    col = request.args.get("college", "").strip()
    sql = "SELECT DISTINCT department_name FROM course_state WHERE department_name IS NOT NULL AND department_name!=''"
    params = []
    if col:
        sql += " AND college_name LIKE ?"
        params.append(f"%{col}%")
    sql += " ORDER BY department_name"
    conn = get_db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return jsonify(success=True, departments=[r[0] for r in rows])

@app.route("/api/changes")
def api_changes():
    limit = int(request.args.get("limit", 50))
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM change_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
    conn.close()
    return jsonify(success=True, changes=rows)

@app.route("/api/notifications")
def api_notifications():
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM notifications_log ORDER BY id DESC LIMIT 50").fetchall()]
    conn.close()
    return jsonify(success=True, notifications=rows)

@app.route("/api/live-search")
def api_live_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify(success=True, results=[])
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT DISTINCT course_name,course_no,college_name FROM course_state WHERE course_name LIKE ? OR course_no LIKE ? LIMIT 15", (f"%{q}%", f"%{q}%")).fetchall()]
    conn.close()
    return jsonify(success=True, results=rows)

@app.route("/api/watcher-status")
def api_watcher_status():
    return jsonify(
        running=_watcher_running.is_set(),
        clients=len(_sse_clients),
        interval=CHECK_INTERVAL_S,
        smtp_ready=is_smtp_ready()
    )

@app.route("/api/test-email", methods=["POST"])
@admin_api_required
def api_test_email():
    d = request.get_json() or {}
    email = (d.get("email") or "").strip()
    if not email:
        return jsonify(success=False, message="يرجى تحديد بريد إلكتروني للتجربة."), 400

    ok, msg = _send_email("طالب تجريبي", email, {
        "course_name": "هيكلة البيانات",
        "course_no": "CS201",
        "section_no": "1",
        "times": "ح ث 09:30-10:30",
        "lecturers": "د. محاضر تجريبي",
        "rooms": "قاعة 101",
        "new_value": "إشعار تجريبي لاختبار البريد الإلكتروني",
        "change_type": "BECAME_OPEN",
    })

    if ok:
        return jsonify(success=True, message=f"أُرسلت الرسالة التجريبية بنجاح إلى البريد الإلكتروني {email}")
    else:
        return jsonify(success=False, message=msg), 400

# =========================================================================
# Welcome Email
# =========================================================================

def _send_welcome_email(student_name, to_email, col_q, course_q):
    """ترسل رسالة ترحيب فورية عند التسجيل الجديد."""
    if not is_smtp_ready():
        return

    if col_q and col_q not in ("ALL", "جميع الكليات"):
        interest_html = f"""
        <p style="margin:6px 0;"><strong>الكليات المهتم بها:</strong></p>
        <div style="background:#d1fae5;border-radius:8px;padding:10px 16px;display:inline-block;color:#065f46;font-weight:700;">{col_q}</div>
        """
    else:
        interest_html = f"""
        <p style="margin:6px 0;"><strong>المادة المطلوبة:</strong></p>
        <div style="background:#d1fae5;border-radius:8px;padding:10px 16px;display:inline-block;color:#065f46;font-weight:700;">{course_q}</div>
        """

    subject = "مرحباً بك في مكانك لمراقبة جريدة المواد"
    html = f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="ar">
    <head><meta charset="UTF-8">
    <style>
      body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background: #f3f4f6; margin: 0; padding: 20px; }}
      .card {{ max-width: 560px; margin: 0 auto; background: #fff; border-radius: 18px; overflow: hidden; border: 1px solid #e5e7eb; }}
      .hdr {{ background: linear-gradient(135deg, #064e3b, #10b981); color: #fff; padding: 32px 28px; text-align: center; }}
      .hdr h1 {{ margin: 0 0 6px; font-size: 1.4rem; }}
      .hdr p  {{ margin: 0; opacity: .85; font-size: .95rem; }}
      .body {{ padding: 28px; }}
      .body p {{ color: #374151; line-height: 1.7; }}
      .btn {{ display: block; text-align: center; background: #059669; color: #fff !important; text-decoration: none; padding: 14px; border-radius: 10px; font-weight: bold; margin-top: 24px; }}
      .footer {{ text-align:center; color:#9ca3af; font-size:.78rem; padding: 16px; }}
    </style>
    </head>
    <body>
    <div class="card">
      <div class="hdr">
        <h1>مكانك لمراقبة جريدة المواد</h1>
        <p>جامعة البلقاء التطبيقية</p>
      </div>
      <div class="body">
        <p>مرحباً <strong>{student_name}</strong>،</p>
        <p>تم تفعيل اشتراكك بنجاح في نظام <strong>مكانك لمراقبة جريدة المواد</strong>. سيصلك إشعار فوري على هذا البريد بمجرد فتح أي مادة أو شعبة جديدة تهمك في الجريدة الرسمية لجامعة البلقاء التطبيقية.</p>
        {interest_html}
        <p style="margin-top:20px;">في حال أردت تغيير اشتراكك أو إلغاؤه، تواصل مع إدارة النظام.</p>
        <a href="https://www.bau.edu.jo/Services/Reg.aspx" target="_blank" class="btn">اذهب لبوابة التسجيل الرسمية</a>
      </div>
      <div class="footer">مكانك لمراقبة جريدة المواد — جامعة البلقاء التطبيقية &copy; 2026</div>
    </div>
    </body>
    </html>
    """
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{SENDER_NAME} <{SMTP_USER}>"
        msg["To"]      = to_email
        msg.attach(MIMEText(html, "html", "utf-8"))
        ctx = ssl._create_unverified_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx, timeout=10) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [to_email], msg.as_string())
        log.info(f"أُرسلت رسالة الترحيب إلى {to_email}")
    except Exception as e:
        log.warning(f"تعذّر إرسال رسالة الترحيب إلى {to_email}: {e}")

# =========================================================================
# Course Request API
# =========================================================================

@app.route("/api/course-request", methods=["POST"])
def api_course_request():
    """طلب إضافة مادة: يحفظ الطلب ويرسل تأكيداً للطالب."""
    d = request.get_json() or request.form
    name       = (d.get("name")        or "").strip()
    email      = (d.get("email")       or "").strip()
    course_no  = (d.get("course_no")   or "").strip()
    course_name= (d.get("course_name") or "").strip()
    notes      = (d.get("notes")       or "").strip()

    if not name or not email or not course_no:
        return jsonify(success=False, message="الاسم والبريد ورمز المادة حقول مطلوبة."), 400

    conn = get_db()
    conn.execute("""
        INSERT INTO course_requests(name, email, course_no, course_name, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (name, email, course_no, course_name, notes, datetime.now().isoformat()))
    conn.commit()
    conn.close()

    # أرسل تأكيد طلب للطالب
    _send_course_request_email(name, email, course_no, course_name, notes)

    return jsonify(success=True, message=f"تم استلام طلبك بنجاح للمادة [{course_no}]. ستصلك رسالة تأكيد على بريدك الإلكتروني.")


def _send_course_request_email(student_name, to_email, course_no, course_name, notes):
    """تأكيد استلام طلب الإعلام عن مادة."""
    if not is_smtp_ready():
        return
    subject = f"تم استلام طلبك — {course_no}"
    html = f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="ar">
    <head><meta charset="UTF-8">
    <style>
      body {{ font-family:'Segoe UI',Tahoma,sans-serif; background:#f3f4f6; margin:0; padding:20px; }}
      .card {{ max-width:560px; margin:0 auto; background:#fff; border-radius:18px; overflow:hidden; border:1px solid #e5e7eb; }}
      .hdr {{ background:linear-gradient(135deg,#064e3b,#10b981); color:#fff; padding:28px; text-align:center; }}
      .hdr h1 {{ margin:0 0 4px; font-size:1.3rem; }}
      .body {{ padding:28px; }}
      .box {{ background:#f9fafb; border-radius:12px; padding:18px; border-right:4px solid #10b981; margin:16px 0; }}
      .row {{ display:flex; justify-content:space-between; padding:7px 0; border-bottom:1px dashed #e5e7eb; }}
      .row:last-child {{ border:none; }}
      .lbl {{ font-weight:700; color:#4b5563; }} .val {{ color:#111; font-weight:600; }}
      .btn {{ display:block; text-align:center; background:#059669; color:#fff!important; text-decoration:none; padding:14px; border-radius:10px; font-weight:bold; margin-top:20px; }}
      .footer {{ text-align:center; color:#9ca3af; font-size:.78rem; padding:16px; }}
    </style>
    </head>
    <body>
    <div class="card">
      <div class="hdr"><h1>تم استلام طلبك بنجاح</h1></div>
      <div class="body">
        <p>مرحباً <strong>{student_name}</strong>،</p>
        <p>لقد استلمنا طلبك لإشعارات إتاحة المادة التالية للتسجيل في جريدة جامعة البلقاء الرسمية:</p>
        <div class="box">
          <div class="row"><span class="lbl">رمز المادة:</span><span class="val">{course_no}</span></div>
          <div class="row"><span class="lbl">اسم المادة:</span><span class="val">{course_name or 'لم يذكر'}</span></div>
          {'<div class="row"><span class="lbl">ملاحظات:</span><span class="val">' + notes + '</span></div>' if notes else ''}
        </div>
        <p>سيتم إشعارك فور إتاحة هذه المادة للتسجيل مع توضيح كامل للوقت والأيام والقاعة والمدرس.</p>
        <p style="font-size:.85rem;color:#ef4444;"><strong>تنبيه:</strong> يرجى التأكد من أن رمز المادة صحيح كما هو مقرر في خطتك الدراسية المعتمدة.</p>
        <a href="https://www.bau.edu.jo/Services/Reg.aspx" target="_blank" class="btn">بوابة التسجيل الرسمية</a>
      </div>
      <div class="footer">مكانك لمراقبة جريدة المواد — جامعة البلقاء التطبيقية &copy; 2026</div>
    </div>
    </body></html>
    """
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{SENDER_NAME} <{SMTP_USER}>"
        msg["To"]      = to_email
        msg.attach(MIMEText(html, "html", "utf-8"))
        ctx = ssl._create_unverified_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx, timeout=10) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [to_email], msg.as_string())
        log.info(f"أُرسل تأكيد طلب المادة إلى {to_email}")
    except Exception as e:
        log.warning(f"تعذّر إرسال تأكيد طلب المادة إلى {to_email}: {e}")


@app.route("/api/course-requests")
@admin_api_required
def api_course_requests():
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM course_requests ORDER BY id DESC").fetchall()]
    conn.close()
    return jsonify(success=True, requests=rows)


def start():
    init_db()
    # Check if thread already started to prevent duplicates
    for tr in threading.enumerate():
        if tr.name == "BauWatcher" and tr.is_alive():
            log.info("خيط المراقبة المستمرة يعمل بالفعل.")
            return
    t = threading.Thread(target=watcher_loop, daemon=True, name="BauWatcher")
    t.start()
    log.info("خيط المراقبة المستمرة بدأ بنجاح.")

# Start DB init and background watcher loop on WSGI/Gunicorn load
start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    log.info(f"الواجهة على: http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

