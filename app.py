import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import hashlib
from typing import Dict, List, Tuple, Optional, Set

# Google Sheets API ile bağlantı için gerekli ayarlar
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
DEFAULT_SHEET_KEY = "1WogWAT7rt6MANHORr2gd5E787Q_Zo0KtfrQkU1Tazfk"
DEFAULT_ATTENDANCE_WORKSHEET_NAME = "Yoklama"
DEFAULT_STUDENTS_WORKSHEET_NAME = "Ogrenciler"

# Service Account ile bağlantı
def get_gspread_client():
    """Google Sheets API Client oluşturur."""
    service_info = st.secrets["gcp_service_account"]
    credentials = Credentials.from_service_account_info(service_info, scopes=SCOPES)
    return gspread.authorize(credentials)

@st.cache_data(show_spinner=False)
def load_students() -> pd.DataFrame:
    """Öğrenci listesini yükler."""
    try:
        sheet_key = st.secrets["sheet"].get("key", DEFAULT_SHEET_KEY)
        worksheet_name = st.secrets["sheet"].get("students_worksheet", DEFAULT_STUDENTS_WORKSHEET_NAME)
        gc = get_gspread_client()
        ws = gc.open_by_key(sheet_key).worksheet(worksheet_name)
        df = pd.DataFrame(ws.get_all_records())
    except Exception as e:
        st.error(f"Veri okunamadı: {e}")
        return pd.DataFrame(columns=["OgrenciID", "AdSoyad", "Grup", "Koc", "Aktif", "UyelikDurumu"])
    
    # Canonicalize column names
    df = _canonicalize_columns(df)
    return df

# Kullanıcı bilgilerini secrets.toml'den alıyoruz
@st.cache_data(show_spinner=False)
def load_users_from_secrets() -> Dict[str, Dict]:
    creds = st.secrets.get("credentials", {})
    return {k: dict(v) for k, v in creds.items()}

def verify_password(users: Dict[str, Dict], username: str, password: str) -> bool:
    """Kullanıcı adı ve şifreyi doğrula"""
    if username not in users:
        st.error(f"Kullanıcı adı bulunamadı: {username}")
        return False
    
    user_info = users[username]
    
    # Şifreyi düz metinle kontrol edelim
    expected_plain = user_info.get("password", "")

    # Eğer şifre boşsa hata verelim
    if password is None or password == "":
        st.error("Şifre boş olamaz!")
        return False

    st.write(f"Giriş yapılan şifre: {password}")  # Debug: Şifreyi kontrol et
    st.write(f"Beklenen şifre: {expected_plain}")  # Debug: Beklenen şifreyi kontrol et

    return password == expected_plain

# Giriş yapma paneli
def login_view(users: Dict[str, Dict]) -> Tuple[str, bool]:
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
        st.info("Bu kullanıcıya atanmış aktif öğrenci bulunamadı.")
        return

    st.markdown("### ✅ Yoklama Listesi")
    present_map = {}
    note_map = {}

    for row in df_students.itertuples(index=False):
        sid = str(row.OgrenciID)
        student_label = f"{row.AdSoyad} — (ID: {sid}) | Grup: {row.Grup}"
        radio_key = f"att_{date_str}_{sid}"
        note_key = f"note_{date_str}_{sid}"

        default_present = False
        if radio_key not in st.session_state:
            st.session_state[radio_key] = "✔️ VAR" if default_present else "✖️ YOK"

        info_col, choice_col = st.columns([3, 2])
        with info_col:
            st.markdown(f"**{student_label}**")
        with choice_col:
            st.radio("Yoklama durumu", ["✔️ VAR", "✖️ YOK"], key=radio_key, horizontal=True)

        present_map[sid] = st.session_state.get(radio_key) == "✔️ VAR"

        if note_key not in st.session_state:
            st.session_state[note_key] = ""

        with st.expander("Not (isteğe bağlı)", expanded=False):
            st.text_input("Not", key=note_key)
        note_map[sid] = st.session_state.get(note_key, "")

    if st.button("💾 Yoklamayı Kaydet", type="primary", use_container_width=True):
        records = []
        for sid, present in present_map.items():
            records.append({
                "Tarih": date_str,
                "Grup": df_students.loc[df_students["OgrenciID"] == sid, "Grup"].values[0],
                "OgrenciID": sid,
                "AdSoyad": df_students.loc[df_students["OgrenciID"] == sid, "AdSoyad"].values[0],
                "Koc": username,
                "Katildi": bool(present),
                "Not": note_map.get(sid),
            })
        append_yoklama_rows(records)
        st.success("Yoklama kaydedildi.")

def main():
    st.title("📋 Yoklama – Koç Telefon Paneli")
    st.caption("Öğrenciler 'Ogrenciler' sekmesinden okunur; yoklamalar 'Yoklama' sekmesine kaydedilir.")
    
    users = load_users_from_secrets()
    if not users:
        st.warning("Kullanıcı listesi boş görünüyor.")
    
    username = st.session_state.get("auth_user")
    if not username:
        _, ok = login_view(users)
        if not ok:
            st.info("Devam etmek için giriş yapın.")
            st.stop()
        username = st.session_state.get("auth_user")

    # Diğer uygulama işlemleri burada devam eder

if __name__ == "__main__":
    main()
