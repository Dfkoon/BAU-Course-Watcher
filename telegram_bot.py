# -*- coding: utf-8 -*-
"""
telegram_bot.py — نظام إشعارات ومساعد الذكاء الاصطناعي الذكي لجريدة مواد جامعة البلقاء التطبيقية (BAU)
يدعم:
1) الربط التلقائي والاشتراك المباشر (One-Click).
2) الرد الذكي والاستعلام المباشر عن أي مادة في الجريدة (مطروحة؟ كم شعبة؟ مفتوحة أم مغلقة؟ أوقاتها؟ مدرسها؟).
"""
import os, uuid, logging, sqlite3, requests, json, re
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

def normalize_arabic_search(text):
    if not text: return ""
    text = re.sub(r'[إأآا]', 'ا', text)
    text = re.sub(r'ة', 'ه', text)
    text = re.sub(r'ى', 'ي', text)
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    # تنظيف كلمات الاستفهام الشائعة لاستخلاص اسم المادة
    stop_words = ["هل", "مادة", "ماده", "شعبة", "شعبه", "مطروحة", "مطروحه", "نازلة", "نازله", "مفتوحة", "مفتوحه", "مغلقة", "مغلقه", "فتحت", "سكرت", "كم", "في", "من", "بدي", "عن", "شو", "ايش", "بدي اعرف", "دكتور", "دكتورة", "قاعة", "وقت", "موعد"]
    for sw in stop_words:
        text = re.sub(rf'\b{sw}\b', '', text)
    return text.strip()

def search_courses_in_db(query):
    """بحث ذكي وسريع في قاعدة بيانات الجريدة الرسمية الحالية."""
    clean_q = normalize_arabic_search(query)
    raw_q = query.strip()
    
    conn = _get_conn()
    # 1. بحث بالرمز المباشر أو الاسم الأصلي
    rows = conn.execute("""
        SELECT * FROM course_state 
        WHERE course_name LIKE ? OR course_no LIKE ? OR lecturers LIKE ?
        ORDER BY status ASC, section_no ASC
        LIMIT 10
    """, (f"%{raw_q}%", f"%{raw_q}%", f"%{raw_q}%")).fetchall()

    # 2. إذا لم يجد، ابحث بالكلمات المنظفة
    if not rows and clean_q and len(clean_q) >= 2:
        words = [w for w in clean_q.split() if len(w) >= 2]
        if words:
            like_clauses = " AND ".join(["course_name LIKE ?" for _ in words])
            params = [f"%{w}%" for w in words]
            rows = conn.execute(f"""
                SELECT * FROM course_state 
                WHERE {like_clauses}
                ORDER BY status ASC, section_no ASC
                LIMIT 10
            """, params).fetchall()

    conn.close()
    return [dict(r) for r in rows]

telegram_bp = Blueprint("telegram_bp", __name__)

@telegram_bp.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    data    = request.get_json(silent=True) or {}
    message = data.get("message", {})
    text    = (message.get("text") or "").strip()
    chat_id = message.get("chat", {}).get("id")
    first_name = message.get("from", {}).get("first_name", "طالب")

    if not chat_id: return "OK", 200

    # ─────────────────────────────────────────────────────────────
    # 1. معالجة أمر /start والربط الفوري (Deep Linking)
    # ─────────────────────────────────────────────────────────────
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

        if not row:
            row = conn.execute("SELECT * FROM telegram_links WHERE chat_id=? ORDER BY created_at DESC LIMIT 1", (str(chat_id),)).fetchone()

        if not row:
            latest_sub = conn.execute("SELECT email, name FROM subscribers ORDER BY id DESC LIMIT 1").fetchone()
            if latest_sub:
                row = {"email": latest_sub["email"], "link_token": token or "DIRECT"}

        if row:
            conn.execute("INSERT OR REPLACE INTO telegram_links (email, chat_id, link_token, created_at) VALUES (?, ?, ?, ?)",
                         (row["email"], str(chat_id), row.get("link_token") or "DIRECT", datetime.now().isoformat()))
            conn.commit()

            subs = conn.execute("SELECT * FROM subscribers WHERE email=? AND active=1", (row["email"],)).fetchall()
            
            details_list = []
            for s in subs:
                cq = s["course_query"]
                sq = s["section_query"]
                col = s["college_query"]

                c_info = conn.execute(
                    "SELECT * FROM course_state WHERE (course_name LIKE ? OR course_no LIKE ?) AND (section_no=? OR ?='ALL' OR ?='') LIMIT 1",
                    (f"%{cq}%", f"%{cq}%", sq, sq, sq)
                ).fetchone()

                if c_info:
                    status_text = "🟢 متاحة للتسجيل" if str(c_info["status"]) == "1" else ("🔴 مغلقة (مكتملة)" if str(c_info["status"]) == "3" else "⚪ ملغاة")
                    times_fmt = _format_times_friendly(c_info["times"])
                    item_text = (
                        f"📚 <b>المادة:</b> {c_info['course_name']} (<code>{c_info['course_no']}</code>)\n"
                        f"🔢 <b>الشعبة:</b> {c_info['section_no']}  |  <b>الحالة:</b> {status_text}\n"
                        f"⏰ <b>المواعيد والأيام:</b> {times_fmt}\n"
                        f"👨‍🏫 <b>المدرس:</b> {c_info['lecturers'] or 'غير محدد'}\n"
                        f"🏛️ <b>الكلية:</b> {c_info['college_name'] or '—'}\n"
                        f"🚪 <b>القاعة:</b> {c_info['rooms'] or 'غير محدد'}"
                    )
                else:
                    item_text = f"📚 <b>المادة:</b> {cq}\n🔢 <b>الشعبة:</b> {sq or 'كافة الشعب'}\n🏛️ <b>الكلية:</b> {col or 'جميع الكليات'}"
                
                details_list.append(item_text)

            courses_block = "\n\n────────────────\n\n".join(details_list) if details_list else "كافة المواد المحددة"

            welcome_msg = (
                f"أهلاً بك <b>{first_name}</b> في نظام مكانك لمراقبة جريدة المواد! 🎓\n\n"
                f"✅ <b>تم تفعيل الإشعارات الفورية بنجاح عبر تلغرام!</b>\n\n"
                f"📋 <b>تفاصيل المواد التي تتابعها حالياً:</b>\n\n"
                f"{courses_block}\n\n"
                f"⚡ <b>سنرسل لك تنبيهاً فورياً في نفس ثانية فتح أي شعبة!</b>\n"
                f"💡 <i>ملاحظة: يمكنك في أي وقت كتابة اسم أي مادة للاستفسار عنها وسأجيبك فوراً!</i>"
            )
            send_telegram_message(chat_id, welcome_msg)
            log.info(f"✅ ربط تلغرام وتفاصيل كاملة مرسلة لـ {row['email']} chat_id={chat_id}")
        else:
            send_telegram_message(chat_id,
                f"أهلاً بك <b>{first_name}</b> في بوت مكانك الجامعي الذكي 🎓\n\n"
                f"🔍 <b>أنا مساعدك الذكي لجريدة المواد في جامعة البلقاء التطبيقية!</b>\n\n"
                f"اكتب لي اسم أي مادة أو رمزها للاستفسار عنها، مثلاً:\n"
                f"• <i>هياكل بيانات</i>\n"
                f"• <i>هل مادة تفاضل وتكامل 1 مطروحة؟</i>\n"
                f"• <i>CS201</i>\n"
                f"• <i>هل شعبة الذكاء الاصطناعي مفتوحة؟</i>\n\n"
                f"وسأعطيك كامل تفاصيل الشعب، المواعيد، حالة التسجيل، والمدرس فوراً ⚡"
            )
        conn.close()
        return "OK", 200

    # ─────────────────────────────────────────────────────────────
    # 2. المساعد الذكي للاستفسارات عن الجريدة (Smart AI Query Engine)
    # ─────────────────────────────────────────────────────────────
    results = search_courses_in_db(text)
    
    if not results:
        not_found_msg = (
            f"عذراً يا <b>{first_name}</b>، لم أجد أي مادة مطابقة لبحثك: <b>\"{text}\"</b> في الجريدة الحالية 🔍\n\n"
            f"💡 <b>نصائح للبحث:</b>\n"
            f"1. اكتب رمز المادة مباشرة (مثال: <code>CS201</code> أو <code>EE305</code>).\n"
            f"2. أو اكتب جزءاً أساسياً من اسم المادة (مثال: <i>هياكل</i> أو <i>برمجة كينونية</i>).\n"
            f"3. تأكد من صحة كتابة الاسم."
        )
        send_telegram_message(chat_id, not_found_msg)
        return "OK", 200

    # تجميع النتائج وعرضها بشكل منسق واحترافي
    course_name_main = results[0]["course_name"]
    course_no_main   = results[0]["course_no"]
    college_main     = results[0]["college_name"] or "جامعة البلقاء التطبيقية"

    reply_lines = [
        f"🔍 <b>نتائج الاستعلام عن:</b> <u>{course_name_main}</u> (<code>{course_no_main}</code>)\n"
        f"🏛️ <b>الكلية:</b> {college_main}\n"
        f"📊 <b>إجمالي الشعب المطروحة:</b> {len(results)} شعبة\n"
        f"────────────────"
    ]

    for idx, c in enumerate(results, 1):
        status_badge = "🟢 <b>متاحة للتسجيل</b>" if str(c["status"]) == "1" else ("🔴 <b>مغلقة (مكتملة)</b>" if str(c["status"]) == "3" else "⚪ <b>ملغاة</b>")
        times_fmt = _format_times_friendly(c["times"])
        prof = c["lecturers"] or "غير محدد"
        room = c["rooms"] or "غير محدد"
        sec = c["section_no"]

        reply_lines.append(
            f"<b>الشعبة ({sec}):</b> {status_badge}\n"
            f"  ⏰ <b>المواعيد:</b> {times_fmt}\n"
            f"  👨‍🏫 <b>المدرس:</b> {prof}\n"
            f"  🚪 <b>القاعة:</b> {room}"
        )

    reply_lines.append("────────────────")
    reply_lines.append("💡 <i>لتتبع أي شعبة مغلقة لتصلك رسالة فور فتحها، اضغط زر 'تتبع' في موقع مكانك الجامعي.</i>")

    final_reply = "\n\n".join(reply_lines)
    send_telegram_message(chat_id, final_reply)
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
