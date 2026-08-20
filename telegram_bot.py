# -*- coding: utf-8 -*-
"""
telegram_bot.py — نظام إشعارات تلغرام الذكي لمشروع مكانك الجامعي (BAU Course Watcher)
يدعم تفعيل الاشتراك المباشر ونظام الـ One-Click التلقائي.
"""
import os, uuid, logging, sqlite3, requests
from datetime import datetime
from flask import Blueprint, request, jsonify

log = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN    = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_API_URL      = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
TELEGRAM_BOT_USERNAME = os.environ.get("TELEGRAM_BOT_USERNAME", "Makanak_BAUBot")
_DB_PATH = os.environ.get("DB_PATH", "data.db")

def set_db_path(path):
    global _DB_PATH, TELEGRAM_BOT_TOKEN, TELEGRAM_API_URL
    _DB_PATH = path
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_API_URL   = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

def _get_conn():
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = sqlite3.Row
    return conn

def init_telegram_table(conn=None):
    close_after = conn is None
    if conn is None: conn = _get_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS telegram_links (
        email TEXT PRIMARY KEY, 
        chat_id TEXT,
        link_token TEXT UNIQUE, 
        created_at TEXT)""")
    conn.commit()
    if close_after: conn.close()

def get_or_create_telegram_link(email):
    email = (email or "").lower().strip()
    conn  = _get_conn()
    row   = conn.execute("SELECT * FROM telegram_links WHERE email=?", (email,)).fetchone()
    if row:
        conn.close()
        return dict(row)
    token = str(uuid.uuid4())[:12]
    conn.execute("INSERT INTO telegram_links(email,chat_id,link_token,created_at) VALUES(?,?,?,?)",
                 (email, None, token, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    return {"email": email, "chat_id": None, "link_token": token, "created_at": datetime.now().isoformat()}

def telegram_connect_url(token):
    return f"https://t.me/{TELEGRAM_BOT_USERNAME}?start={token}"

def get_telegram_chat_id(email):
    if not email: return None
    conn = _get_conn()
    row  = conn.execute("SELECT chat_id FROM telegram_links WHERE email=?",
                        ((email or "").lower().strip(),)).fetchone()
    conn.close()
    return row["chat_id"] if row and row["chat_id"] else None

def send_telegram_message(chat_id, text):
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN غير مضبوط")
        return False
    try:
        resp = requests.post(f"{TELEGRAM_API_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
        if not resp.ok: log.warning(f"tg send fail {resp.status_code}: {resp.text}")
        return resp.ok
    except Exception as e:
        log.error(f"telegram send error: {e}")
        return False

def is_telegram_ready(): return bool(TELEGRAM_BOT_TOKEN)

telegram_bp = Blueprint("telegram_bp", __name__)

@telegram_bp.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    data    = request.get_json(silent=True) or {}
    message = data.get("message", {})
    text    = (message.get("text") or "").strip()
    chat_id = message.get("chat", {}).get("id")
    first_name = message.get("from", {}).get("first_name", "طالب")

    if not chat_id: return "OK", 200

    if text.startswith("/start"):
        parts = text.split(" ", 1)
        if len(parts) > 1 and parts[1].strip():
            token = parts[1].strip()
            conn  = _get_conn()
            row   = conn.execute("SELECT * FROM telegram_links WHERE link_token=?", (token,)).fetchone()
            
            # في حال لم يجد التوكن مباشرة، افحص subscribers
            if not row:
                sub_row = conn.execute("SELECT email, name FROM subscribers WHERE email LIKE ? ORDER BY id DESC LIMIT 1", (f"%{token}%",)).fetchone()
                if sub_row:
                    row = {"email": sub_row["email"], "link_token": token}

            if row:
                conn.execute("INSERT OR REPLACE INTO telegram_links (email, chat_id, link_token, created_at) VALUES (?, ?, ?, ?)",
                             (row["email"], str(chat_id), token, datetime.now().isoformat()))
                conn.commit()

                # جلب أسماء المواد التي يتابعها الطالب
                subs = conn.execute("SELECT course_query, section_query FROM subscribers WHERE email=? AND active=1", (row["email"],)).fetchall()
                courses_text = "\n".join([f"  • <b>{s['course_query']}</b> (شعبة: {s['section_query'] or 'الكل'})" for s in subs]) if subs else "كافة المواد المحددة"

                welcome_msg = (
                    f"أهلاً بك <b>{first_name}</b> في نظام مكانك الجامعي! 🎓\n\n"
                    f"✅ <b>تم تفعيل الإشعارات الفورية بنجاح عبر تلغرام!</b>\n\n"
                    f"📌 <b>المواد قيد المراقبة الآن:</b>\n{courses_text}\n\n"
                    f"⚡ ستصلك رسالة هنا في نفس ثانية فتح الشعبة أو إتاحتها للتسجيل."
                )
                send_telegram_message(chat_id, welcome_msg)
                log.info(f"✅ ربط تلغرام ناجح: {row['email']} <- chat_id={chat_id}")
            else:
                send_telegram_message(chat_id,
                    f"أهلاً بك {first_name}! 🎓\n\n"
                    f"✅ تم استقبال طلبك بنجاح وسنقوم بإرسال التنبيهات لك فوراً بمجرد فتح أي شعبة."
                )
            conn.close()
        else:
            send_telegram_message(chat_id,
                f"أهلاً بك <b>{first_name}</b> في بوت مكانك لمراقبة جريدة المواد 🎓\n\n"
                f"لتفعيل تتبع مادة معينة، اختر المادة من موقع مكانك الجامعي واضغط زر <b>تفعيل الإشعار عبر تلغرام</b> ليتم ربط حسابك تلقائياً."
            )
    else:
        send_telegram_message(chat_id,
            "هذا البوت مخصص لإرسال إشعارات فتح الشعب والمواد لحظة بلحظة ⚡\n"
            "لتغيير المواد التي تتابعها، يرجى استخدام موقع مكانك الجامعي."
        )
    return "OK", 200

@telegram_bp.route("/api/test-telegram", methods=["POST"])
def api_test_telegram():
    d     = request.get_json(silent=True) or {}
    email = (d.get("email") or "").strip()
    if not email: return jsonify(success=False, message="أدخل الإيميل"), 400
    chat_id = get_telegram_chat_id(email)
    if not chat_id: return jsonify(success=False, message="هذا الحساب غير مربوط بتلغرام بعد"), 400
    ok = send_telegram_message(chat_id, "🔔 تجربة ناجحة! إشعارات مكانك الجامعي تعمل فوراً وبشكل سليم عبر تلغرام.")
    if ok: return jsonify(success=True, message="أُرسلت الرسالة التجريبية بنجاح")
    return jsonify(success=False, message="فشل الإرسال، تأكد من TELEGRAM_BOT_TOKEN"), 500
