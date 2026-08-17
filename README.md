<div dir="rtl">

# 🎓 نظام مراقبة جريدة المواد — جامعة البلقاء التطبيقية (BAU Course Watcher)

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/framework-Flask-green.svg)](https://flask.palletsprojects.com/)
[![Security](https://img.shields.io/badge/security-2FA%20TOTP-emerald.svg)](https://github.com/pyauth/pyotp)
[![License](https://img.shields.io/badge/license-MIT-amber.svg)](LICENSE)

نظام ذكي متكامل ومستقل 100% لمراقبة جريدة المواد في **جامعة البلقاء التطبيقية (BAU)** لحظة بلحظة، وإرسال تنبيهات بريدية فورية للطلاب بمجرد توفر شاغر في الشعب المغلقة أو فتح شعب دراسية جديدة.

---

## 🌟 المميزات الرئيسية

- 📡 **مراقبة آلية 24/7:** فحص متواصل ومباشر لجريدة المواد عبر سيرفرات جامعة البلقاء لجميع الكليات والأقسام بدون إشارات كاذبة.
- ⚡ **إشعارات بريدية فورية:** إرسال إيميل تلقائي فور فتح أي شعبة مادة مسجل فيها الطالب.
- 🎯 **فلترة مخصصة للطلاب:** إمكانية الاشتراك بمتابعة كليات محددة، مواد معينة، أو رقم شعبة خاص.
- 🕒 **توقيت 12 ساعة واضح:** تحويل مواعيد الشعب المعقدة إلى توقيت 12 ساعة سلس (صباحاً/مساءً) مع تحديد طبيعة المادة (وجاهي / مدمج / إلكتروني).
- 🔐 **حماية وأمان عالي للأدمن (2FA TOTP):**
  - تسجيل دخول بـ **اسم مستخدم + كلمة مرور مشفرة بـ bcrypt**.
  - مصادقة ثنائية **2FA TOTP** متوافقة مع Google Authenticator و Authy.
  - صفحة إعدادات شاملة للأدمن لتغيير البيانات وتفعيل/إيقاف 2FA مع مولد رمز QR تلقائي.
- 📱 **واجهة مستجيبة بالكامل (Mobile & Desktop Responsive):** تصميم فاخر باللون البيج والزمردي الرسمي لجامعة البلقاء.

---

## 🚀 طريقة التثبيت والتشغيل المحلي

### 1. استنساخ المستودع
```bash
git clone https://github.com/Dfkoon/BAU-Course-Watcher.git
cd BAU-Course-Watcher
```

### 2. إنشاء البيئة الافتراضية وتثبيت المكتبات
```bash
python3 -m venv venv
source venv/bin/activate  # في نظام Linux/macOS
# أو في Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### 3. إعداد ملف البيئة `.env`
قم بنسخ ملف `.env.example` إلى `.env` وتحديث البيانات:
```bash
cp .env.example .env
```

قم بتحديث بيانات البريد الإلكتروني (Gmail App Password) وكلمة مرور الأدمن:
```env
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_gmail_app_password
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_secure_password
PORT=5050
```

### 4. تشغيل السيرفر
```bash
python3 app.py
```
افتح المتصفح على الرابط:
- 🎓 **صفحة الطلاب:** `http://127.0.0.1:5050/student`
- 🔐 **بوابة الأدمن:** `http://127.0.0.1:5050/admin`

---

## 📁 هيكلية المشروع

```text
BAU-Course-Watcher/
├── app.py                 # المحرك الرئيسي لنظام المراقبة والسيرفر (Flask Engine)
├── requirements.txt       # المكتبات والاعتمادات المطلوبة
├── .env.example           # قالب إعدادات المتغيرات البيئية
├── static/
│   └── style.css          # ملف التنسيق الرئيسي (BAU Emerald Theme & Responsive CSS)
└── templates/
    ├── student.html       # واجهة الطلاب والاشتراك في التنبيهات
    ├── login.html         # بوابة تسجيل دخول الأدمن المكونة من خطوتين (2-Step 2FA)
    ├── admin_settings.html# صفحة إعدادات الأدمن والمصادقة الثنائية (TOTP QR)
    ├── index.html         # لوحة تحكم الأدمن وبث التغيرات الحي (SSE Live Feed)
    └── unsubscribed.html  # صفحة إلغاء الاشتراك للطلاب
```

---

## 🔐 نظام الأمان وبوابة التحكم

- **تشفير كلمات المرور:** باستخدام `bcrypt` لحفظ بيانات الاعتماد بأمان تام.
- **تأكيد 2FA:** يمكن للأدمن تفعيل رمز المصادقة الثنائية عبر فتح صفحة `/admin/settings` ومسح الـ QR Code بتطبيق **Google Authenticator**.

---

## 🤝 المساهمة والتطوير
المشروع متاح ومطور لصالح طلاب **جامعة البلقاء التطبيقية**. لتقديم اقتراحات أو الإبلاغ عن مشاكل، يرجى فتح **Issue** أو تقديم **Pull Request**.

---

## 📜 الترخيص
المشروع مرخص تحت رخصة **MIT License**.

</div>
