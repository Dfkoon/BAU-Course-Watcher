# -*- coding: utf-8 -*-
"""
telegram_bot.py — نظام إشعارات تلغرام الذكي لمشروع مكانك الجامعي (BAU Course Watcher)
يعرض تفاصيل المادة، رقم الشعبة، الأيام والأوقات، القاعة والمدرس فور الاشتراك.
"""
import os, uuid, logging, sqlite3, requests, json
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

def _format_times_friendly(times_raw):
    if not times_raw: return "غير محدد"
    try:
        arr = json.loads(times_raw) if isinstance(times_raw, str) and times_raw.startswith("[") else []
        if arr:
            day_map = {"1": "أحد", "2": "إثنين", "3": "ثلاثاء", "4": "أربعاء", "5": "خميس"}
            formatted = []
            for item in arr:
                days = " - ".join([day_map.get(d, d) for d in item.get("days", [])])
                f_time = item.get("from_time", "")
                t_time = item.get("to_time", "")
                formatted.append(f"{days} ({f_time} - {t_time})")
            return " | ".join(formatted)
    except Exception:
        pass
    return str(times_raw)

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
        token = parts[1].strip() if len(parts) > 1 else None
        conn  = _get_conn()

        row = None
        if token:
            row = conn.execute("SELECT * FROM telegram_links WHERE link_token=?", (token,)).fetchone()
            if not row:
                sub_row = conn.execute("SELECT email, name FROM subscribers WHERE email LIKE ? ORDER BY id DESC LIMIT 1", (f"%{token}%",)).fetchone()
                if sub_row:
                    row = {"email": sub_row["email"], "link_token": token}

        # إذا لم يكن هناك توكن أو لم يُعثر عليه، ابحث عن آخر اشتراك مربوط بهذا الشات أو غير مفعل
        if not row:
            row = conn.execute("SELECT * FROM telegram_links WHERE chat_id=? ORDER BY created_at DESC LIMIT 1", (str(chat_id),)).fetchone()

        if not row:
            # خذ آخر اشتراك مضاف في النظام
            latest_sub = conn.execute("SELECT email, name FROM subscribers ORDER BY id DESC LIMIT 1").fetchone()
            if latest_sub:
                row = {"email": latest_sub["email"], "link_token": token or "DIRECT"}

        if row:
            conn.execute("INSERT OR REPLACE INTO telegram_links (email, chat_id, link_token, created_at) VALUES (?, ?, ?, ?)",
                         (row["email"], str(chat_id), row.get("link_token") or "DIRECT", datetime.now().isoformat()))
            conn.commit()

            # جلب تفاصيل كاملة للمواد والشعب المسجل فيها
            subs = conn.execute("SELECT * FROM subscribers WHERE email=? AND active=1", (row["email"],)).fetchall()
            
            details_list = []
            for s in subs:
                cq = s["course_query"]
                sq = s["section_query"]
                col = s["college_query"]

                # ابحث في course_state لجلب كامل بيانات المادة (الأوقات، المدرس، القاعة، الحالة)
                c_info = conn.execute(
                    "SELECT * FROM course_state WHERE (course_name LIKE ? OR course_no LIKE ?) AND (section_no=? OR ?='ALL' OR ?='') LIMIT 1",
                    (f"%{cq}%", f"%{cq}%", sq, sq, sq)
                ).fetchone()

                if c_info:
                    status_text = "🟢 متاحة" if str(c_info["status"]) == "1" else ("🔴 مغلقة" if str(c_info["status"]) == "3" else "⚪ ملغاة")
                    times_fmt = _format_times_friendly(c_info["times"])
                    item_text = (
                        f"📚 <b>المادة:</b> {c_info['course_name']} (<code>{c_info['course_no']}</code>)\n"
                        f"🔢 <b>الشعبة:</b> {c_info['section_no']}  |  <b>الحالة الحالية:</b> {status_text}\n"
                        f"⏰ <b>المواعيد والأيام:</b> {times_fmt}\n"
                        f"👨‍🏫 <b>المدرس:</b> {c_info['lecturers'] or 'غير محدد'}\n"
                        f"🏛️ <b>الكلية:</b> {c_info['college_name'] or '—'} - {c_info['department_name'] or '—'}\n"
                        f"🚪 <b>القاعة:</b> {c_info['rooms'] or 'غير محدد'}"
                    )
                else:
                    item_text = f"📚 <b>المادة:</b> {cq}\n🔢 <b>الشعبة:</b> {sq or 'كافة الشعب'}\n🏛️ <b>الكلية:</b> {col or 'جميع الكليات'}"
                
                details_list.append(item_text)

            courses_block = "\n\n────────────────\n\n".join(details_list) if details_list else "كافة المواد المحددة"

            welcome_msg = (
                f"أهلاً بك <b>{first_name}</b> في نظام مكانك لمراقبة جريدة المواد! 🎓\n\n"
                f"✅ <b>تم تفعيل الإشعارات الفورية بنجاح عبر تلغرام!</b>\n\n"
                f"📋 <b>تفاصيل المادة قيد المراقبة:</b>\n\n"
                f"{courses_block}\n\n"
                f"⚡ <b>سنرسل لك تنبيهاً فورياً في نفس الثانية التي تفتح فيها الشعبة أو يتاح التسجيل بها!</b>"
            )
            send_telegram_message(chat_id, welcome_msg)
            log.info(f"✅ ربط تلغرام وتفاصيل كاملة مرسلة لـ {row['email']} chat_id={chat_id}")
        else:
            send_telegram_message(chat_id,
                f"أهلاً بك <b>{first_name}</b> في بوت مكانك الجامعي 🎓\n\n"
                f"لتفعيل تتبع مادة معينة بكامل تفاصيلها، افتح موقع مكانك الجامعي واضغط زر <b>تتبع</b> بجانب المادة المطلوبة ليتم ربطك تلقائياً."
            )
        conn.close()
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
