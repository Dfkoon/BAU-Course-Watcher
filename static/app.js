/* ==========================================================================
   BAU Live Watcher — Client JS
   يتصل بـ SSE Stream ويعرض التغييرات فوراً بدون ريفريش
   خالي تماماً من الأيقونات التعبيرية (Emoji-Free)
   ========================================================================== */

const CHANGE_LABELS = {
  BECAME_OPEN:      "أصبحت متاحة للتسجيل",
  NEW_SECTION:      "شعبة جديدة متاحة",
  BECAME_CLOSED:    "أُغلقت الشعبة",
  TIMES_CHANGED:    "تغيّر الوقت",
  LECTURER_CHANGED: "تغيّر المدرس",
  ROOM_CHANGED:     "تغيّرت القاعة",
};

let sseSource = null;
let feedCount = 0;

/* ── Boot ─────────────────────────────────────────────────────────────────── */
document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initForm();
  initAutocomplete();
  connectSSE();
  loadCourses();
  loadSubs();
  loadHistory();
  loadColleges();
});

/* ── SSE Connection ───────────────────────────────────────────────────────── */
function connectSSE() {
  if (sseSource) sseSource.close();

  sseSource = new EventSource("/api/stream");
  const badge = document.getElementById("liveBadge");
  const dot   = badge.querySelector(".live-dot");
  const label = document.getElementById("liveLabel");

  sseSource.onopen = () => {
    badge.className = "live-badge connected";
    dot.className   = "live-dot";
    label.textContent = "متصل — يستقبل التغييرات مباشرة";
    toast("اتصال ناجح", "النظام الآن يراقب جريدة البلقاء بشكل مستمر", "success");
  };

  sseSource.onerror = () => {
    badge.className = "live-badge error";
    dot.className   = "live-dot offline";
    label.textContent = "انقطع الاتصال — سيُعاد تلقائياً...";
    setTimeout(connectSSE, 3000);
  };

  sseSource.addEventListener("course_change", e => {
    const ch = JSON.parse(e.data);
    addFeedCard(ch);
    refreshCourseRow(ch);
    if (ch.change_type === "BECAME_OPEN" || ch.change_type === "NEW_SECTION") {
      toast(ch.course_name, `شعبة ${ch.section_no} — ${CHANGE_LABELS[ch.change_type]}`, "success");
    } else if (ch.change_type === "TIMES_CHANGED") {
      toast(ch.course_name, `شعبة ${ch.section_no} — تغيّر الوقت`, "warn");
    }
  });

  sseSource.addEventListener("stats_update", e => {
    applyStats(JSON.parse(e.data));
  });

  sseSource.addEventListener("watcher_error", e => {
    const d = JSON.parse(e.data);
    toast("خطأ في المراقبة", d.message, "error");
  });
}

/* ── Feed Card ────────────────────────────────────────────────────────────── */
function addFeedCard(ch) {
  const empty = document.getElementById("feedEmpty");
  const list  = document.getElementById("feedList");
  if (empty) empty.style.display = "none";

  feedCount++;
  const card = document.createElement("div");
  card.className = `feed-card ${ch.change_type}`;
  const label = CHANGE_LABELS[ch.change_type] || ch.change_type;
  const timeStr = new Date(ch.timestamp).toLocaleTimeString("ar-JO");

  card.innerHTML = `
    <div class="fc-badge">${label}</div>
    <div class="fc-name">${ch.course_name} <span style="color:#9ca3af;font-weight:600">(${ch.course_no})</span></div>
    <div class="fc-detail">شعبة ${ch.section_no} · ${ch.college_name || ''}</div>
    ${ch.new_value ? `<div class="fc-detail" style="margin-top:3px">→ <strong>${ch.new_value}</strong></div>` : ""}
    ${ch.times ? `<div class="fc-detail">${ch.times}</div>` : ""}
    <div class="fc-time"><i class="fa-regular fa-clock"></i> ${timeStr}</div>
  `;

  list.prepend(card);
  while (list.children.length > 60) list.removeChild(list.lastChild);
}

function clearFeed() {
  document.getElementById("feedList").innerHTML = "";
  document.getElementById("feedEmpty").style.display = "flex";
  feedCount = 0;
}

/* ── Stats ────────────────────────────────────────────────────────────────── */
function applyStats(d) {
  document.getElementById("sTotal").textContent   = fmt(d.total_tracked);
  document.getElementById("sOpen").textContent    = fmt(d.open_courses);
  document.getElementById("sClosed").textContent  = fmt(d.closed_courses);
  document.getElementById("sSubs").textContent    = fmt(d.subscribers_count);
  document.getElementById("sChanges").textContent = fmt(d.changes_today || 0);
  document.getElementById("sNotifs").textContent  = fmt(d.notifications_count);

  if (d.last_check_time) {
    const t = new Date(d.last_check_time).toLocaleTimeString("ar-JO");
    document.getElementById("lastUpdateText").textContent = `آخر فحص: ${t}`;
  }

  fetch("/api/watcher-status")
    .then(r => r.json())
    .then(ws => {
      document.getElementById("footerClients").textContent =
        `${ws.clients} متصفح متصل · فحص كل ${ws.interval}ث`;
    }).catch(() => {});
}

function fmt(n) { return Number(n).toLocaleString("ar-JO"); }

/* ── Tabs ─────────────────────────────────────────────────────────────────── */
function initTabs() {
  document.querySelectorAll(".tab").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
      document.querySelectorAll(".pane").forEach(p => p.classList.remove("active"));
      btn.classList.add("active");
      const id = btn.getAttribute("data-tab");
      document.getElementById(id).classList.add("active");
      if (id === "courses")     loadCourses();
      if (id === "subscribers") loadSubs();
      if (id === "history")     loadHistory();
    });
  });
}

/* ── Multi College Selection Logic ────────────────────────────────────────── */
function toggleCollegeDropdown() {
  const drop = document.getElementById("collegeDropdown");
  drop.classList.toggle("hidden");
}

document.addEventListener("click", e => {
  const box = document.querySelector(".multi-college-box");
  if (box && !box.contains(e.target)) {
    const drop = document.getElementById("collegeDropdown");
    if (drop) drop.classList.add("hidden");
  }
});

function toggleAllColleges(chkAll) {
  const list = document.getElementById("collegeCheckboxList");
  const checkboxes = list.querySelectorAll("input[type='checkbox']");
  checkboxes.forEach(c => c.checked = chkAll.checked);
  updateCollegeButtonText();
}

function onCollegeCheckboxChange() {
  const chkAll = document.getElementById("chkAllColleges");
  const list = document.getElementById("collegeCheckboxList");
  const checkboxes = Array.from(list.querySelectorAll("input[type='checkbox']"));
  const checkedCount = checkboxes.filter(c => c.checked).length;

  if (checkedCount === checkboxes.length) {
    chkAll.checked = true;
  } else {
    chkAll.checked = false;
  }
  updateCollegeButtonText();
}

function updateCollegeButtonText() {
  const btnText = document.getElementById("collegeSelectBtnText");
  const chkAll = document.getElementById("chkAllColleges");
  const list = document.getElementById("collegeCheckboxList");
  const checkboxes = Array.from(list.querySelectorAll("input[type='checkbox']"));
  const checked = checkboxes.filter(c => c.checked).map(c => c.value);

  if (chkAll.checked || checked.length === checkboxes.length || checked.length === 0) {
    btnText.textContent = "جميع كليات الجامعة (متابعة عامة)";
  } else if (checked.length === 1) {
    btnText.textContent = checked[0];
  } else {
    btnText.textContent = `تم تحديد (${checked.length}) كليات`;
  }
}

function getSelectedColleges() {
  const chkAll = document.getElementById("chkAllColleges");
  if (chkAll.checked) return "ALL";
  const list = document.getElementById("collegeCheckboxList");
  const checked = Array.from(list.querySelectorAll("input[type='checkbox']"))
                       .filter(c => c.checked)
                       .map(c => c.value);
  if (!checked.length) return "ALL";
  return checked.join(", ");
}

/* ── Subscribe Form ───────────────────────────────────────────────────────── */
function initForm() {
  document.getElementById("subForm").addEventListener("submit", async e => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const obj = Object.fromEntries(fd.entries());
    obj.college_query = getSelectedColleges();

    try {
      const res = await fetch("/api/subscribe", {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify(obj)
      });
      const data = await res.json();
      if (data.success) {
        toast("تم التسجيل", data.message, "success");
        e.target.reset();
        document.getElementById("chkAllColleges").checked = true;
        toggleAllColleges(document.getElementById("chkAllColleges"));
        loadSubs();
      } else {
        toast("خطأ", data.message, "error");
      }
    } catch { toast("خطأ", "تعذر الاتصال بالسيرفر", "error"); }
  });
}

/* ── Autocomplete ─────────────────────────────────────────────────────────── */
function initAutocomplete() {
  const inp  = document.getElementById("cqInput");
  const drop = document.getElementById("acDrop");
  let timer;

  inp.addEventListener("input", () => {
    clearTimeout(timer);
    const q = inp.value.trim();
    if (q.length < 2) { drop.classList.add("hidden"); return; }
    timer = setTimeout(async () => {
      try {
        const res  = await fetch(`/api/live-search?q=${encodeURIComponent(q)}`);
        const data = await res.json();
        if (data.results?.length) {
          drop.innerHTML = data.results.map(r =>
            `<div class="ac-item" onclick="pickAC('${r.course_name.replace(/'/g,"\\'")}')">
              <span>${r.course_name}</span>
              <span class="no">${r.course_no}</span>
            </div>`
          ).join("");
          drop.classList.remove("hidden");
        } else drop.classList.add("hidden");
      } catch { drop.classList.add("hidden"); }
    }, 250);
  });

  document.addEventListener("click", e => {
    if (!drop.contains(e.target) && e.target !== inp) drop.classList.add("hidden");
  });
}

function pickAC(name) {
  document.getElementById("cqInput").value = name;
  document.getElementById("acDrop").classList.add("hidden");
}

/* ── Courses Table ────────────────────────────────────────────────────────── */
async function loadColleges() {
  const selFilter = document.getElementById("cCollege");
  const chkList   = document.getElementById("collegeCheckboxList");

  const res = await fetch("/api/colleges").then(r=>r.json()).catch(()=>({}));
  const colleges = res.colleges || [];

  if (selFilter) selFilter.innerHTML = '<option value="">كل الكليات</option>';
  if (chkList) chkList.innerHTML = '';

  colleges.forEach(c => {
    if (selFilter) {
      const opt = document.createElement("option");
      opt.value = c; opt.textContent = c;
      selFilter.appendChild(opt);
    }

    if (chkList) {
      const label = document.createElement("label");
      label.className = "college-checkbox-item";
      label.innerHTML = `
        <input type="checkbox" value="${c}" checked onchange="onCollegeCheckboxChange()">
        <span>${c}</span>
      `;
      chkList.appendChild(label);
    }
  });

  updateCollegeButtonText();
}

async function loadDepartments() {
  const deptSel = document.getElementById("cDept");
  if (!deptSel) return;
  const col = document.getElementById("cCollege")?.value || "";
  const { departments = [] } = await fetch(`/api/departments?college=${encodeURIComponent(col)}`).then(r => r.json());
  deptSel.innerHTML = '<option value="">كل الأقسام الأكاديمية</option>' +
    departments.map(d => `<option value="${d}">${d}</option>`).join("");
}

function formatTime12h(timeStr) {
  if (!timeStr || timeStr === '—') return '—';
  const dayMap = {'ح':'الأحد', 'ن':'الإثنين', 'ث':'الثلاثاء', 'ر':'الأربعاء', 'خ':'الخميس', 'س':'السبت'};
  const convertHhmm = (hhmm) => {
    const parts = hhmm.split(':');
    if (parts.length < 2) return hhmm;
    let h = parseInt(parts[0], 10);
    const m = parts[1];
    const period = h < 12 ? 'ص' : 'م';
    h = h > 12 ? h - 12 : (h === 0 ? 12 : h);
    return `${h}:${m} ${period}`;
  };

  const re = /((?:[حنثرخس]\s*)+)(\d{2}:\d{2})\s+(\d{2}:\d{2})/g;
  let match;
  const parts = [];
  while ((match = re.exec(timeStr)) !== null) {
    const daysRaw = match[1].trim().split(/\s+/);
    const daysList = daysRaw.map(d => dayMap[d] || d);
    const daysFmt = daysList.join(' - ');
    parts.push(`<div style="white-space:nowrap;margin:2px 0;font-size:.84rem;"><strong>${daysFmt}:</strong> <span style="color:#047857;font-weight:700;">${convertHhmm(match[2])} - ${convertHhmm(match[3])}</span></div>`);
  }
  return parts.length ? parts.join('') : timeStr;
}

async function loadCourses() {
  const tb   = document.getElementById("coursesTb");
  const q    = document.getElementById("cSearch")?.value?.trim()  || "";
  const col  = document.getElementById("cCollege")?.value         || "";
  const dept = document.getElementById("cDept")?.value            || "";
  const st   = document.getElementById("cStatus")?.value           || "";
  const url  = `/api/courses?query=${encodeURIComponent(q)}&college=${encodeURIComponent(col)}&dept=${encodeURIComponent(dept)}&status=${encodeURIComponent(st)}`;

  tb.innerHTML = `<tr><td colspan="10" class="empty"><i class="fa-solid fa-spinner fa-spin"></i> جاري التحميل...</td></tr>`;
  try {
    const { courses = [] } = await fetch(url).then(r => r.json());
    if (!courses.length) {
      tb.innerHTML = `<tr><td colspan="10" class="empty">لا توجد نتائج — قد تحتاج لانتظار أول دورة فحص (45ث)</td></tr>`;
      return;
    }

    const formatRemarks = r => {
      if (!r) return '<span style="color:#9ca3af">—</span>';
      if (r.includes('وجاهي')) return '<span class="badge" style="background:#dcfce7;color:#15803d;border:1px solid #86efac;">وجاهي</span>';
      if (r.includes('مدمج'))  return '<span class="badge" style="background:#e0e7ff;color:#3730a3;border:1px solid #a5b4fc;">مدمج</span>';
      if (r.includes('إلكتروني') || r.toLowerCase().includes('online')) return '<span class="badge" style="background:#f3e8ff;color:#6b21a8;border:1px solid #d8b4fe;">إلكتروني</span>';
      return `<span class="badge" style="background:#f3f4f6;color:#374151;">${r}</span>`;
    };

    tb.innerHTML = courses.map(c => {
      const [bcls, btxt] = c.status === "1" ? ["open","متاحة"]
                         : c.status === "2" ? ["cancelled","ملغاة"]
                         : ["closed","مغلقة"];
      const cNameEsc = c.course_name.replace(/'/g, "\\'");
      const colNameEsc = (c.college_name || "").replace(/'/g, "\\'");
      return `<tr id="row-${c.course_key}">
        <td><strong>${c.course_no}</strong></td>
        <td><strong>${c.course_name}</strong></td>
        <td>${c.section_no}</td>
        <td><span class="badge ${bcls}">${btxt}</span></td>
        <td>${formatRemarks(c.remarks)}</td>
        <td>${formatTime12h(c.times)}</td>
        <td>${c.lecturers||'—'}</td>
        <td>${c.rooms||'—'}</td>
        <td><strong>${c.college_name||''}</strong><br><small style="color:#059669;font-weight:700;"><i class="fa-solid fa-layer-group"></i> ${c.department_name||'عام'}</small></td>
        <td><button class="btn-sm" onclick="trackThis('${cNameEsc}','${c.section_no}','${colNameEsc}')"><i class="fa-solid fa-bell"></i> تتبع</button></td>
      </tr>`;
    }).join("");
  } catch {
    tb.innerHTML = `<tr><td colspan="10" class="empty">خطأ في التحميل</td></tr>`;
  }
}

function refreshCourseRow(ch) {
  const row = document.getElementById(`row-${ch.course_key}`);
  if (!row) return;
  const cells = row.querySelectorAll("td");
  if (cells.length < 7) return;
  const [bcls, btxt] = ch.status === "1" ? ["open","متاحة"]
                     : ch.status === "2" ? ["cancelled","ملغاة"]
                     : ["closed","مغلقة"];
  cells[3].textContent = ch.times || "—";
  cells[4].textContent = ch.lecturers || "—";
  cells[5].textContent = ch.rooms || "—";
  cells[6].innerHTML   = `<span class="badge ${bcls}">${btxt}</span>`;

  row.style.background = "#fef9c3";
  setTimeout(() => row.style.background = "", 2000);
}

function trackThis(name, section, college) {
  document.querySelector('[data-tab="subscribe"]').click();
  if (college) {
    const list = document.getElementById("collegeCheckboxList");
    const checkboxes = list.querySelectorAll("input[type='checkbox']");
    checkboxes.forEach(c => c.checked = (c.value === college));
    document.getElementById("chkAllColleges").checked = false;
    updateCollegeButtonText();
  }
  document.getElementById("cqInput").value = name;
  document.querySelector('[name="section_query"]').value = section;
  toast("تم تعبئة النموذج", `المادة: ${name} — الشعبة: ${section}`, "info");
}

/* ── Subscribers ──────────────────────────────────────────────────────────── */
async function loadSubs() {
  const tb = document.getElementById("subsTb");
  tb.innerHTML = `<tr><td colspan="8" class="empty">جاري التحميل...</td></tr>`;
  const { subscribers: list = [] } = await fetch("/api/subscribers").then(r=>r.json()).catch(()=>({}));
  if (!list.length) {
    tb.innerHTML = `<tr><td colspan="8" class="empty">لا يوجد مشتركون بعد</td></tr>`; return;
  }
  tb.innerHTML = list.map((s,i) => {
    const colDisp = (!s.college_query || s.college_query === "ALL") ? "جميع الكليات" : s.college_query;
    return `<tr>
      <td>${i+1}</td>
      <td><strong>${s.name}</strong></td>
      <td>${s.email}</td>
      <td><span class="badge open">${colDisp}</span></td>
      <td><strong>${s.course_query}</strong></td>
      <td>${s.section_query ? `شعبة ${s.section_query}` : 'كل الشعب'}</td>
      <td><span class="badge ${s.active?'active':'inactive'}">${s.active?'نشط':'موقف'}</span></td>
      <td style="display:flex;gap:6px">
        <button class="btn-sm" onclick="toggleSub(${s.id})">${s.active?'إيقاف':'تشغيل'}</button>
        <button class="btn-danger" onclick="deleteSub(${s.id})">حذف</button>
      </td>
    </tr>`;
  }).join("");
}

async function toggleSub(id) {
  await fetch(`/api/subscribers/toggle/${id}`, {method:"POST"});
  loadSubs();
}
async function deleteSub(id) {
  if (!confirm("حذف هذا الاشتراك؟")) return;
  await fetch(`/api/subscribers/${id}`, {method:"DELETE"});
  toast("تم الحذف", "تم حذف الاشتراك.", "info");
  loadSubs();
}

/* ── Change History ───────────────────────────────────────────────────────── */
async function loadHistory() {
  const tb  = document.getElementById("historyTb");
  tb.innerHTML = `<tr><td colspan="7" class="empty">جاري التحميل...</td></tr>`;
  const { changes = [] } = await fetch("/api/changes?limit=80").then(r=>r.json()).catch(()=>({}));
  if (!changes.length) {
    tb.innerHTML = `<tr><td colspan="7" class="empty">لا يوجد تغييرات بعد — سيظهر أول تغيير بعد أول دورة فحص</td></tr>`;
    return;
  }
  tb.innerHTML = changes.map(c => {
    const lbl = CHANGE_LABELS[c.change_type] || c.change_type;
    const t   = new Date(c.created_at).toLocaleString("ar-JO");
    return `<tr>
      <td style="white-space:nowrap;font-size:12px">${t}</td>
      <td><strong>${c.course_name||c.course_key}</strong></td>
      <td>${c.section_no||'—'}</td>
      <td><span class="badge open" style="white-space:nowrap">${lbl}</span></td>
      <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${c.old_value||''}">${c.old_value||'—'}</td>
      <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${c.new_value||''}"><strong>${c.new_value||'—'}</strong></td>
      <td>${c.college_name||'—'}</td>
    </tr>`;
  }).join("");
}

async function testEmail() {
  const email = prompt("أدخل البريد الإلكتروني للتجربة:");
  if (!email) return;
  const res  = await fetch("/api/test-email", {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({email})
  }).then(r=>r.json()).catch(()=>({success:false}));
  toast(res.success ? "تجربة الإشعار" : "خطأ", res.message || "—", res.success ? "success" : "error");
}

/* ── Toast ────────────────────────────────────────────────────────────────── */
function toast(title, msg, type="success") {
  const icons = {success:"fa-circle-check", error:"fa-circle-exclamation", info:"fa-circle-info", warn:"fa-triangle-exclamation"};
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `
    <i class="fa-solid ${icons[type]||icons.info} toast-icon"></i>
    <div class="toast-body">
      <div class="title">${title}</div>
      <div class="msg">${msg}</div>
    </div>`;
  document.getElementById("toasts").appendChild(el);
  setTimeout(() => {
    el.style.opacity="0";
    el.style.transform="translateX(-20px)";
    el.style.transition="all .3s";
    setTimeout(()=>el.remove(),300);
  }, 4500);
}
