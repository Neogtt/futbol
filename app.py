import os, time, sqlite3, requests
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import streamlit as st
import pandas as pd

# ---------------------------
# Config & Secrets
# ---------------------------
st.set_page_config(page_title="Futbol Okulu • Tahsilat & WhatsApp", layout="wide")

WHATSAPP_TOKEN = st.secrets.get("WHATSAPP_TOKEN", os.getenv("WHATSAPP_TOKEN", ""))
WABA_PHONE_NUMBER_ID = st.secrets.get("WABA_PHONE_NUMBER_ID", os.getenv("WABA_PHONE_NUMBER_ID", ""))  # e.g. "1234567890"
GRAPH_BASE = "https://graph.facebook.com/v20.0"

if "DB_PATH" not in st.session_state:
    st.session_state.DB_PATH = "futbol_okulu.db"

# ---------------------------
# DB Helpers
# ---------------------------
def get_conn():
    return sqlite3.connect(st.session_state.DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ad TEXT,
        soyad TEXT,
        veli_ad TEXT,
        veli_tel TEXT,     -- +90 ile E.164 formatı önerilir
        takim TEXT,
        dogum_tarihi TEXT, -- YYYY-MM-DD
        aktif_mi INTEGER DEFAULT 1
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER,
        donem TEXT,                  -- Örn: 2025-10
        tutar REAL,
        son_odeme_tarihi TEXT,       -- YYYY-MM-DD
        durum TEXT DEFAULT 'bekliyor',  -- bekliyor|odendi|gecikti
        FOREIGN KEY(student_id) REFERENCES students(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER,
        tarih TEXT,         -- YYYY-MM-DD
        tutar REAL,
        aciklama TEXT,
        FOREIGN KEY(invoice_id) REFERENCES invoices(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS msg_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        template_name TEXT,
        msg_type TEXT,     -- template|text
        payload TEXT,
        status TEXT,
        ts TEXT
    )
    """)
    conn.commit()
    conn.close()

init_db()

# ---------------------------
# WhatsApp Cloud API
# ---------------------------
def wa_headers():
    return {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}

def _post_whatsapp_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{GRAPH_BASE}/{WABA_PHONE_NUMBER_ID}/messages"
    try:
        r = requests.post(url, json=payload, headers=wa_headers(), timeout=30)
        try:
            data = r.json() if r.content else {}
        except ValueError:
            data = {"raw": r.text}
        return {"status_code": r.status_code, "data": data}
    except requests.RequestException as exc:
        return {"status_code": None, "error": str(exc), "data": {}}


def send_template(to_phone_e164: str, template_name: str, lang_code="tr", body_params: List[str] = None) -> Dict[str, Any]:
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone_e164,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": lang_code},
            "components": []
        }
    }
    if body_params:
        payload["template"]["components"].append({
            "type": "body",
            "parameters": [{"type": "text", "text": str(x)} for x in body_params]
        })
    return _post_whatsapp_payload(payload)

def send_text(to_phone_e164: str, text: str) -> Dict[str, Any]:
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone_e164,
        "type": "text",
        "text": {"body": text}
    }

    return _post_whatsapp_payload(payload)


def _response_status_label(resp: Dict[str, Any]) -> str:
    code = resp.get("status_code")
    if isinstance(code, int) and 200 <= code < 300:
        return "ok"
    if resp.get("error"):
        return "err_request"
    return f"err_{code if code is not None else 'unknown'}"


def _response_error_message(resp: Dict[str, Any]) -> str:
    if resp.get("error"):
        return str(resp["error"])
    data = resp.get("data")
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            return err.get("message") or str(err)
        if data:
            return str(data)
    return "Bilinmeyen hata"

def log_msg(phone: str, template_name: str, msg_type: str, payload: str, status: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("INSERT INTO msg_log(phone, template_name, msg_type, payload, status, ts) VALUES(?,?,?,?,?,?)",
              (phone, template_name, msg_type, payload, status, datetime.now().isoformat(timespec="seconds")))
    conn.commit()
    conn.close()

# ---------------------------
# Data Helpers
# ---------------------------
def df_students() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM students", conn)
    conn.close()
    return df

def df_invoices(join_students=True) -> pd.DataFrame:
    conn = get_conn()
    if join_students:
        q = """
        SELECT invoices.*, students.ad, students.soyad, students.veli_tel
        FROM invoices
        LEFT JOIN students ON students.id = invoices.student_id
        ORDER BY date(invoices.son_odeme_tarihi) ASC
        """
    else:
        q = "SELECT * FROM invoices ORDER BY date(son_odeme_tarihi) ASC"
    df = pd.read_sql_query(q, conn)
    conn.close()
    return df

def upsert_student(row: dict, row_id: int | None):
    conn = get_conn()
    c = conn.cursor()
    if row_id:
        c.execute("""UPDATE students SET ad=?, soyad=?, veli_ad=?, veli_tel=?, takim=?, dogum_tarihi=?, aktif_mi=?
                     WHERE id=?""",
                  (row["ad"], row["soyad"], row["veli_ad"], row["veli_tel"], row["takim"],
                   row["dogum_tarihi"], int(row.get("aktif_mi",1)), row_id))
    else:
        c.execute("""INSERT INTO students(ad, soyad, veli_ad, veli_tel, takim, dogum_tarihi, aktif_mi)
                     VALUES(?,?,?,?,?,?,?)""",
                  (row["ad"], row["soyad"], row["veli_ad"], row["veli_tel"], row["takim"],
                   row["dogum_tarihi"], int(row.get("aktif_mi",1))))
    conn.commit()
    conn.close()

def add_invoice(student_id: int, donem: str, tutar: float, son_odeme_tarihi: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""INSERT INTO invoices(student_id, donem, tutar, son_odeme_tarihi, durum)
                 VALUES(?,?,?,?, 'bekliyor')""",
              (student_id, donem, float(tutar), son_odeme_tarihi))
    conn.commit()
    conn.close()

def mark_paid(invoice_id: int, tutar: float):
    conn = get_conn()
    c = conn.cursor()
    today = date.today().isoformat()
    c.execute("UPDATE invoices SET durum='odendi' WHERE id=?", (invoice_id,))
    c.execute("INSERT INTO payments(invoice_id, tarih, tutar, aciklama) VALUES(?,?,?,?)",
              (invoice_id, today, float(tutar), "Ödeme alındı"))
    conn.commit()
    conn.close()

def compute_status_rollover():
    """Vadesi geçen 'bekliyor' faturaları 'gecikti' yap."""
    today = date.today().isoformat()
    conn = get_conn()
    c = conn.cursor()
    c.execute("""UPDATE invoices
                 SET durum='gecikti'
                 WHERE durum='bekliyor' AND date(son_odeme_tarihi) < date(?)""", (today,))
    conn.commit()
    conn.close()

# ---------------------------
# UI — Sidebar
# ---------------------------
with st.sidebar:
    st.title("⚽ Futbol Okulu")
    st.caption("Ödeme Takip + WhatsApp")
    st.markdown("---")
    st.subheader("WhatsApp Ayarları")
    st.text_input("WABA_PHONE_NUMBER_ID", value=WABA_PHONE_NUMBER_ID, disabled=True)
    st.text_input("WHATSAPP_TOKEN (st.secrets)", value=("●"*10 if WHATSAPP_TOKEN else "—"), disabled=True)
    st.markdown("""
- İlk mesajlar **şablon** olmalı (24 saat kuralı).
- Gruplara mesaj API ile **gönderilemez**; veli numaralarına toplu gönderim yapılır.
- Numara formatı: **+90XXXXXXXXXX**
    """)
    st.markdown("---")
    if st.button("Vade/Gecikme Durumlarını Güncelle"):
        compute_status_rollover()
        st.success("Durumlar güncellendi.")

# ---------------------------
# UI — Tabs
# ---------------------------
tab_dash, tab_students, tab_invoices, tab_whatsapp, tab_logs, tab_special = st.tabs(
    ["📊 Pano", "👨‍👩‍👧‍👦 Öğrenciler", "🧾 Faturalar", "📲 WhatsApp Gönder", "🧾 Log", "🎉 Özel Günler"]
)

# ---- Dashboard
with tab_dash:
    st.header("📊 Pano")
    df_inv = df_invoices()
    today = date.today()
    this_week_end = today + timedelta(days=7)

    due_soon = df_inv[(pd.to_datetime(df_inv["son_odeme_tarihi"]) >= pd.to_datetime(today)) &
                      (pd.to_datetime(df_inv["son_odeme_tarihi"]) <= pd.to_datetime(this_week_end)) &
                      (df_inv["durum"] == "bekliyor")]
    overdue = df_inv[df_inv["durum"] == "gecikti"]

    c1, c2, c3 = st.columns(3)
    c1.metric("Bu Hafta Vadesi Dolan", len(due_soon))
    c2.metric("Geciken Fatura", len(overdue))
    c3.metric("Toplam Bekleyen", int((df_inv["durum"] == "bekliyor").sum()))

    st.subheader("Bu Hafta Vade")
    st.dataframe(due_soon, use_container_width=True)
    st.subheader("Gecikenler")
    st.dataframe(overdue, use_container_width=True)

# ---- Students
with tab_students:
    st.header("👨‍👩‍👧‍👦 Öğrenciler")
    df = df_students()
    st.dataframe(df, use_container_width=True)
    st.markdown("### Yeni / Güncelle")
    with st.form("student_form"):
        row_id = st.number_input("ID (güncellemek için girin, yeni için boş bırakın)", min_value=0, step=1)
        ad = st.text_input("Ad")
        soyad = st.text_input("Soyad")
        veli_ad = st.text_input("Veli Adı")
        veli_tel = st.text_input("Veli Telefonu (+90...)")
        takim = st.text_input("Takım / Yaş Grubu")
        dogum = st.date_input("Doğum Tarihi", value=date(2015,1,1))
        aktif = st.checkbox("Aktif", value=True)
        submitted = st.form_submit_button("Kaydet")
        if submitted:
            payload = {
                "ad": ad.strip(), "soyad": soyad.strip(),
                "veli_ad": veli_ad.strip(), "veli_tel": veli_tel.strip(),
                "takim": takim.strip(), "dogum_tarihi": dogum.isoformat(),
                "aktif_mi": 1 if aktif else 0
            }
            upsert_student(payload, row_id if row_id>0 else None)
            st.success("Kaydedildi. Sol üstten 'Rerun' yapın veya sayfayı tazeleyin.")

# ---- Invoices
with tab_invoices:
    st.header("🧾 Faturalar")
    df = df_invoices()
    st.dataframe(df, use_container_width=True)

    st.markdown("### Fatura Ekle")
    colA, colB, colC, colD = st.columns(4)
    with colA:
        student_id = st.number_input("Öğrenci ID", min_value=1, step=1)
    with colB:
        donem = st.text_input("Dönem (örn: 2025-10)")
    with colC:
        tutar = st.number_input("Tutar (TL)", min_value=0.0, step=50.0)
    with colD:
        vade = st.date_input("Son Ödeme Tarihi", value=date.today())
    if st.button("Fatura Oluştur"):
        add_invoice(student_id, donem, tutar, vade.isoformat())
        st.success("Fatura eklendi.")

    st.markdown("### Ödeme Al")
    col1, col2 = st.columns(2)
    with col1:
        inv_id = st.number_input("Fatura ID", min_value=1, step=1)
    with col2:
        odeme_tutar = st.number_input("Ödenen Tutar", min_value=0.0, step=50.0)
    if st.button("Ödendi İşaretle"):
        mark_paid(inv_id, odeme_tutar)
        st.success("Fatura ödendi olarak işaretlendi.")

# ---- WhatsApp Send
with tab_whatsapp:
    st.header("📲 WhatsApp Gönder")
    st.markdown("**İlk temas mesajları şablon olmalı.** 24 saat penceresinde serbest metin gönderebilirsiniz.")
    df = df_invoices()
    selectable = df[(df["durum"].isin(["bekliyor", "gecikti"])) & df["veli_tel"].notna()]
    st.markdown("### Hedef Listesi (bekleyen/geciken)")
    st.dataframe(selectable[["id","ad","soyad","donem","tutar","son_odeme_tarihi","durum","veli_tel"]], use_container_width=True)

    st.markdown("#### Şablon Gönder")
    template_name = st.text_input("Template adı", value="tuition_reminder_v1")
    lang_code = st.text_input("Dil kodu", value="tr")
    body_params_raw = st.text_input("Body parametreleri (virgülle: Ali,Ekim 2025,10 Ekim 2025,1500)")
    send_to_ids = st.text_input("Gönderilecek Fatura ID'leri (virgülle: 12,13,15)")
    delay_sec = st.number_input("Mesajlar arası gecikme (sn)", min_value=0, max_value=30, value=2)

    if st.button("Toplu Şablon Gönder"):
        if not (WHATSAPP_TOKEN and WABA_PHONE_NUMBER_ID):
            st.error("WhatsApp ayarları eksik (token / phone number id).")
        else:
            ids = [int(x.strip()) for x in send_to_ids.split(",") if x.strip().isdigit()]
            body_params = [x.strip() for x in body_params_raw.split(",")] if body_params_raw.strip() else []
            sent, failed = 0, 0
            error_msgs: List[str] = []            
            for iid in ids:
                hit = selectable[selectable["id"]==iid]
                if hit.empty:
                    continue
                phone = hit.iloc[0]["veli_tel"]
                resp = send_template(phone, template_name, lang_code, body_params)
                status = _response_status_label(resp)
                log_msg(phone, template_name, "template", str(body_params), status)
                if status=="ok":
                    sent += 1
                else:
                    failed += 1
                    error_msgs.append(f"{phone}: {_response_error_message(resp)}")                    
                time.sleep(delay_sec)
            st.success(f"Tamamlandı. Başarılı: {sent}, Hata: {failed}")
            if error_msgs:
                st.warning("\n".join(["Gönderilemeyenler:"] + [f"- {msg}" for msg in error_msgs]))            

    st.markdown("#### Serbest Metin Gönder (24 saat penceresinde)")
    free_text = st.text_area("Mesaj gövdesi", value="Merhaba, yardımcı olmamızı ister misiniz?")
    send_to_phones = st.text_input("Telefon(lar) (+90… virgülle): +905XXXXXXXXX,+905YYYYYYYY")
    if st.button("Toplu Serbest Metin Gönder"):
        if not (WHATSAPP_TOKEN and WABA_PHONE_NUMBER_ID):
            st.error("WhatsApp ayarları eksik (token / phone number id).")
        else:
            phones = [x.strip() for x in send_to_phones.split(",") if x.strip()]
            sent, failed = 0, 0
            error_msgs: List[str] = []            
            for p in phones:
                resp = send_text(p, free_text)
                status = _response_status_label(resp)
                log_msg(p, "-", "text", free_text, status)
                if status=="ok":
                    sent += 1
                else:
                    failed += 1
                    error_msgs.append(f"{p}: {_response_error_message(resp)}")                    
                time.sleep(1)
            st.success(f"Tamamlandı. Başarılı: {sent}, Hata: {failed}")
            if error_msgs:
                st.warning("\n".join(["Gönderilemeyenler:"] + [f"- {msg}" for msg in error_msgs]))

# ---- Logs
with tab_logs:
    st.header("🧾 Mesaj Kayıtları")
    conn = get_conn()
    df_log = pd.read_sql_query("SELECT * FROM msg_log ORDER BY id DESC LIMIT 500", conn)
    conn.close()
    st.dataframe(df_log, use_container_width=True)

# ---- Special Days
with tab_special:
    st.header("🎉 Özel Gün Mesajları")
    st.caption("Doğum günü ve resmi/kurumsal günler için hızlı gönderim.")
    # Doğum günü bugün olanlar:
    df_s = df_students()
    today_mmdd = (date.today().month, date.today().day)
    df_birth = df_s[df_s["dogum_tarihi"].apply(lambda x: (int(x[5:7]), int(x[8:10])) == today_mmdd if isinstance(x,str) and len(x)>=10 else False)]
    st.subheader("🎂 Bugün doğum günü olan öğrenciler")
    st.dataframe(df_birth, use_container_width=True)

    bmsg = st.text_area("Doğum günü mesajı", value="İyi ki doğdun! 🎂 Antrenmanda minik bir sürprizimiz var. ⚽️✨")
    if "veli_tel" in df_birth.columns:
    bday_phones: List[str] = []
    phones = ""
    if "veli_tel" in df_birth.columns:
        bday_phones = [
            str(x).strip()
            for x in df_birth["veli_tel"].tolist()
            if pd.notna(x) and str(x).strip()
        ]
        phones = ",".join(bday_phones)
    elif not df_birth.empty:
        st.warning("Seçilen öğrenciler için veli telefonu bulunamadı.")
        
    st.text_input("Hedef telefonlar", value=phones, key="bday_phones", disabled=True)

    if st.button("Doğum Günü Mesajlarını Gönder"):
        if not (WHATSAPP_TOKEN and WABA_PHONE_NUMBER_ID):
            st.error("WhatsApp ayarları eksik (token / phone number id).")
        else:
            if not bday_phones:
                st.error("Gönderilecek veli telefonu bulunamadı.")
                st.stop()            
            sent = failed = 0
            error_msgs: List[str] = []
            for p in bday_phones:            
                if not p:
                    continue
                resp = send_text(p, bmsg)
                status = _response_status_label(resp)
                log_msg(p, "-", "text", bmsg, status)
                if status=="ok":
                    sent += 1
                else:
                    failed += 1
                    error_msgs.append(f"{p}: {_response_error_message(resp)}")
                time.sleep(1)
            st.success(f"Tamamlandı. Başarılı: {sent}, Hata: {failed}")
            if error_msgs:
                st.warning("\n".join(["Gönderilemeyenler:"] + [f"- {msg}" for msg in error_msgs]))            
