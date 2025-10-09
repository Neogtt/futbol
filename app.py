import os, time, sqlite3, requests
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import streamlit as st
import pandas as pd

# ---------------------------
# Config & Secrets
# ---------------------------
st.set_page_config(page_title="Futbol Okulu • Tahsilat & WhatsApp", layout="wide")

def _get_secret(name: str, default: str = "") -> str:
    try:
        return st.secrets[name]
    except (KeyError, FileNotFoundError):
        return os.getenv(name, default)


WHATSAPP_TOKEN = _get_secret("WHATSAPP_TOKEN")
WABA_PHONE_NUMBER_ID = _get_secret("WABA_PHONE_NUMBER_ID")  # e.g. "1234567890"
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
        aktif_mi INTEGER DEFAULT 1,
        uye_tipi TEXT DEFAULT 'Aylık'
    )
    """)
    # Eski veritabanları için üyelik sütununu ekle
    c.execute("PRAGMA table_info(students)")
    columns = [row[1] for row in c.fetchall()]
    if "uye_tipi" not in columns:
        c.execute("ALTER TABLE students ADD COLUMN uye_tipi TEXT DEFAULT 'Aylık'")
    c.execute("""
        CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ad TEXT UNIQUE
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

def df_groups() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM groups ORDER BY ad COLLATE NOCASE ASC", conn)
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
        c.execute("""UPDATE students SET ad=?, soyad=?, veli_ad=?, veli_tel=?, takim=?, dogum_tarihi=?, aktif_mi=?, uye_tipi=?
                     WHERE id=?""",
                 (row["ad"], row["soyad"], row["veli_ad"], row["veli_tel"], row.get("takim", ""),
                  row["dogum_tarihi"], int(row.get("aktif_mi",1)), row.get("uye_tipi", "Aylık"), row_id))
    else:
        c.execute("""INSERT INTO students(ad, soyad, veli_ad, veli_tel, takim, dogum_tarihi, aktif_mi, uye_tipi)
                     VALUES(?,?,?,?,?,?,?,?)""",
                 (row["ad"], row["soyad"], row["veli_ad"], row["veli_tel"], row.get("takim", ""),
                  row["dogum_tarihi"], int(row.get("aktif_mi",1)), row.get("uye_tipi", "Aylık")))
    conn.commit()
    conn.close()

def delete_student(row_id: int) -> bool:
    if not row_id:
        return False
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM students WHERE id=?", (row_id,))
    conn.commit()
    deleted = c.rowcount > 0
    conn.close()
    return deleted

def add_group(name: str):
    name = name.strip()
    if not name:
        return False
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("INSERT OR IGNORE INTO groups(ad) VALUES(?)", (name,))
        conn.commit()
        return c.rowcount > 0
    finally:
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


    group_success = st.session_state.pop("group_success", None)
    if group_success:
        st.success(group_success)

    student_success = st.session_state.pop("student_success", None)
    if student_success:
        st.success(student_success)

    df = df_students()
    st.dataframe(df, use_container_width=True)

    st.markdown("### Gruplar")
    df_g = df_groups()
    if df_g.empty:
        st.info("Henüz grup eklenmedi. Aşağıdaki formu kullanarak yeni gruplar oluşturabilirsiniz.")
    else:
        st.dataframe(df_g, use_container_width=True)

    with st.form("group_form"):
        new_group = st.text_input("Yeni Grup Adı")
        group_submitted = st.form_submit_button("Grup Ekle")
        if group_submitted:
            if add_group(new_group):
                st.session_state["group_success"] = "Grup eklendi. Liste yenilendi."
                st.rerun()
            else:
                st.warning("Grup adı boş olamaz veya zaten mevcut.")

    st.markdown("### Yeni / Güncelle")
    with st.form("student_form"):
        student_records = df.to_dict("records")
        select_options = {"— Yeni Öğrenci —": None}
        for row in student_records:
            label = f"#{int(row['id'])} • {str(row.get('ad') or '').strip()} {str(row.get('soyad') or '').strip()}"
            select_options[label] = row

        selected_label = st.selectbox(
            "ID (güncellemek için seçin)",
            options=list(select_options.keys()),
            index=0,
        )
        selected_student = select_options.get(selected_label)

        row_id = int(selected_student["id"]) if selected_student and selected_student.get("id") else 0
        st.number_input(
            "Seçilen Öğrenci ID", min_value=0, step=1, value=row_id, disabled=True
        )

        ad_default = str(selected_student.get("ad", "")) if selected_student else ""
        soyad_default = str(selected_student.get("soyad", "")) if selected_student else ""
        veli_ad_default = str(selected_student.get("veli_ad", "")) if selected_student else ""
        veli_tel_default = str(selected_student.get("veli_tel", "")) if selected_student else ""

        ad = st.text_input("Ad", value=ad_default)
        soyad = st.text_input("Soyad", value=soyad_default)
        veli_ad = st.text_input("Veli Adı", value=veli_ad_default)
        veli_tel = st.text_input("Veli Telefonu (+90...)", value=veli_tel_default)
        group_names = df_g["ad"].tolist()
        if group_names:
            takim_default = ""
            if selected_student:
                takim_default = str(selected_student.get("takim", "") or "")
            takim_options = [""] + group_names
            if takim_default and takim_default not in takim_options:
                takim_options.append(takim_default)
            takim_index = takim_options.index(takim_default) if takim_default in takim_options else 0
            takim = st.selectbox(
                "Grup Seçin",
                options=takim_options,
                index=takim_index,
                format_func=lambda x: "— Grup seçin —" if x == "" else x,
            )
        else:
            takim_default = str(selected_student.get("takim", "")) if selected_student else ""
            takim = st.text_input("Grup (önce yukarıdan grup ekleyin)", value=takim_default)

        default_dogum = date(2015, 1, 1)
        if selected_student:
            dogum_val = selected_student.get("dogum_tarihi")
            if isinstance(dogum_val, str) and dogum_val:
                try:
                    default_dogum = date.fromisoformat(dogum_val)
                except ValueError:
                    pass
        dogum = st.date_input("Doğum Tarihi", value=default_dogum)

        aktif_default = True
        if selected_student:
            aktif_val = selected_student.get("aktif_mi", 1)
            try:
                aktif_default = bool(int(aktif_val))
            except (TypeError, ValueError):
                aktif_default = True
        aktif = st.checkbox("Aktif", value=aktif_default)
        uye_options = ["Aylık", "3 Aylık", "6 Aylık", "Senelik"]
        uye_tipi = st.selectbox(
            "Üyelik Süresi",
            options=uye_options,
            index=(
                uye_options.index(selected_student.get("uye_tipi", "Aylık"))
                if selected_student and selected_student.get("uye_tipi") in uye_options
                else 0
            ),
        )
        submitted = st.form_submit_button("Kaydet")
        pending_key = "pending_delete_student"
        if pending_key in st.session_state:
            if int(st.session_state[pending_key]) <= 0 or int(row_id) <= 0:
                st.session_state.pop(pending_key, None)
            elif st.session_state[pending_key] != int(row_id):
                st.session_state.pop(pending_key, None)

        pending_for = st.session_state.get(pending_key)
        show_confirm = pending_for and int(row_id) > 0 and pending_for == int(row_id)
        if show_confirm:
            st.warning("Seçili öğrenciyi silmek istediğinizden emin misiniz?", icon="⚠️")
            confirm_delete = st.form_submit_button("Evet, Sil", type="primary")
            cancel_delete = st.form_submit_button("Vazgeç")
            if confirm_delete:
                if delete_student(int(row_id)):
                    st.session_state.pop(pending_key, None)
                    st.session_state["student_success"] = "Öğrenci kaydı silindi. Liste yenilendi."
                    st.rerun()
                else:
                    st.warning("Belirtilen ID ile öğrenci bulunamadı.")
                    st.session_state.pop(pending_key, None)
                    st.rerun()
            elif cancel_delete:
                st.session_state.pop(pending_key, None)
                st.rerun()
        else:
            if st.form_submit_button("Seçili Öğrenciyi Sil", type="primary"):
                if int(row_id) <= 0:
                    st.warning("Silmek için geçerli bir ID girin.")
                else:
                    st.session_state[pending_key] = int(row_id)
                    st.rerun()
        if submitted:
            payload = {
                "ad": ad.strip(), "soyad": soyad.strip(),
                "veli_ad": veli_ad.strip(), "veli_tel": veli_tel.strip(),
                "takim": takim.strip(), "dogum_tarihi": dogum.isoformat(),
                "uye_tipi": uye_tipi,
                "aktif_mi": 1 if aktif else 0
            }
            upsert_student(payload, row_id if row_id>0 else None)
            st.session_state["student_success"] = "Öğrenci kaydı kaydedildi. Liste yenilendi."
            st.rerun()
        if submitted and pending_key in st.session_state:
            st.session_state.pop(pending_key, None)
                    
# ---- Invoices
with tab_invoices:
    st.header("🧾 Faturalar")
    invoice_success = st.session_state.pop("invoice_success", None)
    if invoice_success:
        st.success(invoice_success)

    payment_success = st.session_state.pop("payment_success", None)
    if payment_success:
        st.success(payment_success)

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
        st.session_state["invoice_success"] = "Fatura eklendi. Liste yenilendi."
        st.rerun()

    st.markdown("### Ödeme Al")
    col1, col2 = st.columns(2)
    with col1:
        inv_id = st.number_input("Fatura ID", min_value=1, step=1)
    with col2:
        odeme_tutar = st.number_input("Ödenen Tutar", min_value=0.0, step=50.0)
    if st.button("Ödendi İşaretle"):
        mark_paid(inv_id, odeme_tutar)
        st.session_state["payment_success"] = "Fatura ödendi olarak işaretlendi. Liste yenilendi."
        st.rerun()

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
    st.markdown("#### Günü Geçmiş Aidatlar")
    overdue = selectable[selectable["durum"] == "gecikti"]
    st.dataframe(
        overdue[["id", "ad", "soyad", "donem", "tutar", "son_odeme_tarihi", "veli_tel"]],
        use_container_width=True,
    )

    overdue_options = {}
    for _, row in overdue.iterrows():
        tutar = float(row.tutar) if pd.notna(row.tutar) else 0.0
        label = (
            f"#{int(row.id)} • {str(row.ad or '').strip()} {str(row.soyad or '').strip()}"
            f" • {row.son_odeme_tarihi} • {tutar:.0f} TL"
        )
        overdue_options[label] = int(row.id)
    selected_labels = st.multiselect(
        "Mesaj gönderilecek kayıtları seçin",
        options=list(overdue_options.keys()),
    )
    selected_ids = [overdue_options[label] for label in selected_labels]

    st.markdown("#### Serbest Metin Gönder (24 saat penceresinde)")

    default_msg = (
        "Sevgili Velimiz, ödenmemiş aidatınız bulunmaktadır. "
        "Lütfen ödemenizi en kısa sürede yapınız."
    )
    free_text = st.text_area("Mesaj gövdesi", value=default_msg)
    if st.button("Seçili Kişilere Mesaj Gönder"):
        if not (WHATSAPP_TOKEN and WABA_PHONE_NUMBER_ID):
            st.error("WhatsApp ayarları eksik (token / phone number id).")
        else:
            if not selected_ids:
                st.warning("Lütfen mesaj göndermek için listeden en az bir kayıt seçin.")
                st.stop()
            phones = [
                str(x)
                for x in overdue[overdue["id"].isin(selected_ids)]["veli_tel"].tolist()
                if pd.notna(x) and str(x).strip()
            ]
            if not phones:
                st.warning("Seçilen kayıtlar için geçerli veli telefonu bulunamadı.")
                st.stop()
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
