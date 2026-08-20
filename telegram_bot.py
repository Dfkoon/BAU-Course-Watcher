# -*- coding: utf-8 -*-
"""telegram_bot.py — نظام إشعارات تلغرام لمشروع مكانك الجامعي"""
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
        email TEXT PRIMARY KEY, chat_id TEXT,
        link_token TEXT UNIQUE, created_at TEXT)""")
    conn.commit()
    if close_after: conn.close()

def get_or_create_telegram_link(email):
    email = (email or "").lower().strip()
    conn  = _get_conn()
    row   = conn.execute("SELECT * FROM telegram_links WHERE email=?", (email,)).fetchone()
    if row:
        conn.close()
        return dict(row)
    token = str(uuid.uuid4())
    conn.execute("INSERT INTO telegram_links(email,chat_id,link_token,created_at) VALUES(?,?,?,?)",
                 (email, None, token, datetime.now().isoformat()))
    conn.commit(); conn.close()
    return {"email": email, "chat_id": None, "link_token": token, "created_at": datetime.now().isoformat()}

def telegram_connect_url(token):
    return f"https://t.me/{TELEGRAM_BOT_USERNAME}?start={token}"

def get_telegram_chat_id(email):
    conn = _get_conn()
    row  = conn.execute("SELECT chat_id FROM telegram_links WHERE email=?",
                        ((email or "").lower().strip(),)).fetchone()
    conn.close()
    return row["chat_id"] if row and row["chat_id"] else None

def send_telegram_message(chat_id, text):
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN غير مضبوط"); return False
    try:
        resp = requests.post(f"{TELEGRAM_API_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
        if not resp.ok: log.warning(f"tg send fail {resp.status_code}: {resp.text}")
        return resp.ok
    except Exception as e:
        log.error(f"telegram send error: {e}"); return False

def is_telegram_ready(): return bool(TELEGRAM_BOT_TOKEN)

telegram_bp = Blueprint("telegram_bp", __name__)

@telegram_bp.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    data    = request.get_json(silent=True) or {}
    message = data.get("message", {})
    text    = (message.get("text") or "").strip()
    chat_id = message.get("chat", {}).get("id")
    if not chat_id: return "OK", 200
    if text.startswith("/start"):
        parts = text.split(" ", 1)
        if len(parts) > 1 and parts[1].strip():
            token = parts[1].strip()
            conn  = _get_conn()
            row   = conn.execute("SELECT * FROM telegram_links WHERE link_token=?", (token,)).fetchone()
            if row:
                conn.execute("UPDATE telegram_links SET chat_id=? WHERE link_token=?", (str(chat_id), token))
                conn.commit()
                send_telegram_message(chat_id,
                    "تم تفعيل الإشعارات الفورية بنجاح ✅\n"
                    "راح توصلك كل التحديثات على المواد اللي تتابعها فوراً هون بدل الإيميل.")
                log.info(f"✅ ربط تلغرام: {row['email']} <- chat_id={chat_id}")
            else:
                send_telegram_message(chat_id,
                    "الرابط غير صالح أو منتهي.\nارجع لموقع مكانك واضغط زر تفعيل الإشعارات من جديد.")
            conn.close()
        else:
            send_telegram_message(chat_id,
                "أهلاً فيك بمكانك الجامعي 👋\n"
                "لتفعيل الإشعارات افتح البوت من رابط التفعيل بصفحة اشتراكك بالموقع.")
    else:
        send_telegram_message(chat_id,
            "هالبوت مخصص للإشعارات الفورية بس.\nلأي تعديل استخدم موقع مكانك الجامعي.")
    return "OK", 200

@telegram_bp.route("/api/test-telegram", methods=["POST"])
def api_test_telegram():
    d     = request.get_json(silent=True) or {}
    email = (d.get("email") or "").strip()
    if not email: return jsonify(success=False, message="أدخل الإيميل"), 400
    chat_id = get_telegram_chat_id(email)
    if not chat_id: return jsonify(success=False, message="هالإيميل لسا ما فعّل تلغرام"), 400
    ok = send_telegram_message(chat_id, "🔔 هاي رسالة تجريبية من مكانك الجامعي.")
    if ok: return jsonify(success=True, message="أُرسلت الرسالة التجريبية بنجاح")
    return jsonify(success=False, message="فشل الإرسال، تأكد من TELEGRAM_BOT_TOKEN"), 500
