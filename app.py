import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError, GSpreadException
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import hashlib
from typing import Dict, List, Tuple

# =============================
# 🔧 UYGULAMA AYARLARI
# =============================
st.set_page_config(page_title="Yoklama – Koç Paneli", layout="wide")

# =============================
# 🔐 KIMLIK DOĞRULAMA & GOOGLE
# =============================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# 👉 Doğrudan KEY ile açıyoruz (kullanıcının verdiği Sheet ID)
DEFAULT_SHEET_KEY = "1EX6e_r6MaPKh6xi03gmOvhVPHFEsSyuB"
DEFAULT_WORKSHEET_NAME = "yoklama"  # Tek sayfa: "Tarih, Grup, OgrenciID, AdSoyad, Koc, Katildi, Not"


def get_sheet_settings() -> Tuple[str, str]:
    """st.secrets içinden sayfa kimliği ve adını okur, yoksa varsayılanı döner."""
    sheet_secrets = st.secrets.get("sheet", {})
    sheet_key = sheet_secrets.get("key", DEFAULT_SHEET_KEY)
    worksheet_name = sheet_secrets.get("worksheet", DEFAULT_WORKSHEET_NAME)
    return sheet_key, worksheet_name

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    """Service Account ile gspread client oluşturur (Streamlit Cloud uyumlu)."""
    service_info = st.secrets["gcp_service_account"]
    credentials = Credentials.from_service_account_info(service_info, scopes=SCOPES)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def open_ws_by_key(sheet_key: str, worksheet_name: str):
    gc = get_gspread_client()
    try:
        sh = gc.open_by_key(sheet_key)
    except PermissionError as exc:
        service_email = st.secrets.get("gcp_service_account", {}).get("client_email", "")
        if service_email:
            detail = (
                "Google Sheet'e erişim izni verilmedi. Lütfen belirtilen sayfayı "
                f"'{service_email}' servis hesabı ile paylaşın."
            )
        else:
            detail = (
                "Google Sheet'e erişim izni verilmedi. Lütfen servis hesabınızın belgeye erişimi "
                "olduğundan emin olun."
            )
        raise PermissionError(detail) from exc
    return sh.worksheet(worksheet_name)

# =============================
# 👤 KULLANICI YÖNETIMI
# =============================
# 2 yöntem desteklenir:
# 1) st.secrets["credentials"] içinde kullanıcılar (hızlı başlangıç)
# 2) "Koc" kolonundaki benzersiz isimlerden otomatik kullanıcı listesi (salt okunur)

@st.cache_data(show_spinner=False)
def load_users_from_secrets() -> Dict[str, Dict]:
    creds = st.secrets.get("credentials", {})
    return {k: dict(v) for k, v in creds.items()}

@st.cache_data(show_spinner=False)
def get_all_users_from_sheet() -> List[str]:
    try:
        sheet_key, worksheet_name = get_sheet_settings()
        ws = open_ws_by_key(sheet_key, worksheet_name)
        rows = ws.get_all_records()
        return sorted({str(r.get("Koc", "")).strip() for r in rows if str(r.get("Koc", "")).strip()})
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def get_all_users() -> Dict[str, Dict]:
    # Öncelik secrets: parola doğrulama gerekir. Aksi halde sheet'teki koç adlarını parola gerektirmeden gösteririz.
    secret_users = load_users_from_secrets()
    if secret_users:
        return secret_users
    # Parolasız mod (saha kullanımını kolaylaştırmak için): sadece kullanıcı adı sorulur
    auto_users = {u: {"password_hash": "", "role": "coach"} for u in get_all_users_from_sheet() if u}
    return auto_users


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def verify_password(users: Dict[str, Dict], username: str, password: str) -> bool:
    if username not in users:
        return False
    expected_hash = users[username].get("password_hash", "")
    # Parolasız mod: expected_hash boş ise şifre kontrolü atlanır
    if not expected_hash:
        return True
    return sha256_hex(password) == expected_hash

# =============================
# 📒 YOKLAMA (Tek Sayfa Şeması)
# =============================
# Beklenen başlıklar: Tarih, Grup, OgrenciID, AdSoyad, Koc, Katildi, Not

@st.cache_data(show_spinner=False)
def load_yoklama() -> pd.DataFrame:
    try:
        sheet_key, worksheet_name = get_sheet_settings()
        ws = open_ws_by_key(sheet_key, worksheet_name)
    except PermissionError as exc:
        st.error(
            "Google Sheet'e erişim izni doğrulanamadı. Lütfen servis hesabınızla belgeyi paylaştığınızdan "
            "emin olun.\n\n"
            f"Detay: {exc}"
        )
        return pd.DataFrame(columns=["Tarih", "Grup", "OgrenciID", "AdSoyad", "Koc", "Katildi", "Not"])        
    except APIError as exc:
        message = str(exc)
        if "This operation is not supported for this document" in message:
            message += (
                "\n\nSeçilen kimlik bir Google E-Tablosu olmayabilir. ID'nin doğru olduğundan ve belgenin Google"
                " Sheet formatında olduğundan emin olun."
            )
        st.error(
            "Google Sheet'e bağlanırken bir hata oluştu. Lütfen kimlik bilgilerinizi ve sayfa erişiminizi kontrol edin.\n\n"
            f"Detay: {message}"
        )
        return pd.DataFrame(columns=["Tarih", "Grup", "OgrenciID", "AdSoyad", "Koc", "Katildi", "Not"])
    except GSpreadException as exc:
        st.error(
            "Google Sheet'e bağlanırken bir hata oluştu. Lütfen kimlik bilgilerinizi ve sayfa erişiminizi kontrol edin.\n\n"
            f"Detay: {exc}"
        )
        return pd.DataFrame(columns=["Tarih", "Grup", "OgrenciID", "AdSoyad", "Koc", "Katildi", "Not"])
        
    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        return pd.DataFrame(columns=["Tarih", "Grup", "OgrenciID", "AdSoyad", "Koc", "Katildi", "Not"])    
    # Normalize
    for col in ["Grup", "OgrenciID", "AdSoyad", "Koc", "Not"]:
        if col in df:
            df[col] = df[col].astype(str).str.strip()
    # Tarih'i tarih tipine çevirmeye çalışma; metin kalabilir. Filtrelemede format kullanacağız.
    if "Katildi" in df:
        df["Katildi"] = df["Katildi"].astype(str).str.lower().isin(["1", "true", "yes", "evet", "var", "✔", "x", "✓", "doğru"]) 
    return df


def append_yoklama_rows(records: List[Dict]):
    try:
        sheet_key, worksheet_name = get_sheet_settings()
        ws = open_ws_by_key(sheet_key, worksheet_name)
    except PermissionError as exc:
        raise RuntimeError(
            "Google Sheet'e erişim izni doğrulanamadı. Lütfen servis hesabınızla belgeyi paylaştığınızdan "
            "emin olun.\n\n"
            f"Detay: {exc}"
        ) from exc        
    except APIError as exc:
        message = str(exc)
        if "This operation is not supported for this document" in message:
            message += (
                "\nBelge bir Google E-Tablosu olmayabilir veya ID yanlış olabilir. Lütfen belgeyi Google Sheets formatına"
                " dönüştürün ve erişim verdiğinizden emin olun."
            )
        raise RuntimeError(
            "Google Sheet'e yazılırken bir hata oluştu. Kimlik bilgilerinizi ve sayfa erişim izinlerinizi doğrulayın."
            f"\n\nDetay: {message}"
        ) from exc
    except GSpreadException as exc:
        raise RuntimeError(
            "Google Sheet'e yazılırken bir hata oluştu. Kimlik bilgilerinizi ve sayfa erişim izinlerinizi doğrulayın."
        ) from exc
        
    # Başlık yoksa yaz
    all_values = ws.get_all_values()
    if not all_values:
        ws.update('A1:G1', [["Tarih", "Grup", "OgrenciID", "AdSoyad", "Koc", "Katildi", "Not"]])
    values = []
    for r in records:
        values.append([
            r.get("Tarih"),
            r.get("Grup", ""),
            r.get("OgrenciID"),
            r.get("AdSoyad"),
            r.get("Koc"),
            "TRUE" if r.get("Katildi") else "FALSE",
            r.get("Not", ""),
        ])
    if values:
        ws.append_rows(values, value_input_option="USER_ENTERED")

# =============================
# 🧭 TÜRETILMIŞ ÖĞRENCİ LİSTESİ (Koç bazlı)
# =============================
@st.cache_data(show_spinner=False)
def get_students_for_coach(username: str) -> pd.DataFrame:
    df = load_yoklama()
    if df.empty:
        return pd.DataFrame(columns=["OgrenciID", "AdSoyad", "Grup", "Koc"])    
    # Bu koçla ilişkili tüm öğrenciler (geçmiş kayıtlarından türetilir)
    df_k = df[df["Koc"].str.lower() == username.lower()].copy()
    if df_k.empty:
        return pd.DataFrame(columns=["OgrenciID", "AdSoyad", "Grup", "Koc"])    
    # Her öğrencinin en güncel "Grup" bilgisini almak için tarih sıralayalım (Tarih metin olabilir; stabilize etmek için sondan alacağız)
    # Aynı OgrenciID + AdSoyad kombinasyonunu tekilleştir.
    df_k["_order"] = range(len(df_k))
    df_k.sort_values("_order", ascending=False, inplace=True)
    df_last = df_k.drop_duplicates(subset=["OgrenciID", "AdSoyad"], keep="first")
    return df_last[["OgrenciID", "AdSoyad", "Grup", "Koc"]].sort_values("AdSoyad")

# =============================
# 📱 ARAYÜZ – KOÇ PANELI
# =============================

def login_view(users: Dict[str, Dict]) -> Tuple[str, bool]:
    st.markdown("""
        <style>
        .big-input input {font-size: 20px; padding: 12px 10px;}
        .big-btn button {font-size: 18px; padding: 10px 16px; border-radius: 10px;}
        </style>
    """, unsafe_allow_html=True)

    st.markdown("### 👋 Koç Girişi")
    usernames = list(users.keys())
    username = st.selectbox("Kullanıcı adı", usernames, index=0 if usernames else None)
    password = st.text_input("Şifre", type="password", key="password", placeholder="••••••••")

    login_ok = False
    if st.button("Giriş Yap", type="primary", use_container_width=True):
        if username and verify_password(users, username.strip(), password):
            st.session_state["auth_user"] = username.strip()
            login_ok = True
        else:
            st.error("Kullanıcı adı veya şifre hatalı.")
    return st.session_state.get("auth_user"), login_ok


def attendance_view(username: str):
    st.markdown(f"#### 👤 Oturum: **{username}**")

    today = date.today()
    selected_date = st.date_input("Tarih", value=today, format="DD.MM.YYYY")
    date_str = selected_date.strftime("%d.%m.%Y")

    df_students = get_students_for_coach(username)
    if df_students.empty:
        st.info("Bu kullanıcıya atanmış öğrenci geçmişi bulunamadı. İlk yoklamayı kaydedince liste oluşacak.")
        return

    # Aynı gün için önceden girilmiş kayıtları çek (prefill)
    df = load_yoklama()
    df_day = df[(df["Koc"].str.lower() == username.lower()) & (df["Tarih"].astype(str) == date_str)]
    pre = {str(r.OgrenciID): (bool(r.Katildi), str(r.Not) if str(r.Not) != "nan" else "") for r in df_day.itertuples(index=False)}

    st.markdown("---")
    st.markdown("### ✅ Yoklama Listesi")

    colA, colB = st.columns([1,1])
    with colA:
        select_all = st.checkbox("Hepsini **VAR** (✔️) işaretle", value=False)
    with colB:
        clear_all = st.checkbox("Hepsini **YOK** (✖️) yap", value=False)

    if select_all and clear_all:
        st.warning("Lütfen yalnız birini seçin: Hepsini VAR **veya** Hepsini YOK.")

    present_map = {}
    note_map = {}

    for row in df_students.itertuples(index=False):
        sid = str(row.OgrenciID)
        label = f"{row.AdSoyad} — (ID: {sid}) | Grup: {row.Grup}"
        default_present = pre.get(sid, (False, ""))[0]
        default_note = pre.get(sid, (False, ""))[1]
        if select_all:
            default_present = True
        if clear_all:
            default_present = False
        present_map[sid] = st.checkbox(label, value=default_present, key=f"cb_{sid}")
        with st.expander("Not (isteğe bağlı)", expanded=False):
            note_map[sid] = st.text_input("Not", value=default_note, key=f"note_{sid}")

    st.markdown("---")
    grup_default = df_students["Grup"].mode().iloc[0] if not df_students["Grup"].empty else ""
    genel_not = st.text_input("Günün genel notu (opsiyonel)", value="")

    if st.button("💾 Yoklamayı Kaydet", type="primary", use_container_width=True):
        now_iso = datetime.now().isoformat(timespec="seconds")
        records = []
        sid_to_name = dict(zip(df_students["OgrenciID"].astype(str), df_students["AdSoyad"]))
        sid_to_group = dict(zip(df_students["OgrenciID"].astype(str), df_students["Grup"]))
        for sid, present in present_map.items():
            records.append({
                "Tarih": date_str,
                "Grup": sid_to_group.get(sid, grup_default),
                "OgrenciID": sid,
                "AdSoyad": sid_to_name.get(sid, ""),
                "Koc": username,
                "Katildi": bool(present),
                "Not": note_map.get(sid) or genel_not,
                "timestamp": now_iso,
            })
        try:
            append_yoklama_rows(records)
            load_yoklama.clear()
            get_students_for_coach.clear()
            st.success("Yoklama kaydedildi.")
        except Exception as e:
            st.error(f"Yoklama yazılamadı: {e}")

# =============================
# 🔲 ANA ÇALIŞMA AKIŞI
# =============================

def main():
    st.title("📋 Yoklama – Koç Telefon Paneli")
    st.caption("Tek sayfa: 'yoklama' – Tarih, Grup, OgrenciID, AdSoyad, Koc, Katildi, Not")

    users = get_all_users()
    if not users:
        st.warning("Kullanıcı listesi boş görünüyor. 'Koc' sütununda en az bir isim olduğundan veya st.secrets[credentials] tanımlandığından emin olun.")

    username = st.session_state.get("auth_user")
    if not username:
        _, ok = login_view(users)
        if not ok:
            st.info("Devam etmek için giriş yapın.")
            st.stop()
        username = st.session_state.get("auth_user")

    attendance_view(username)


if __name__ == "__main__":
    main()
