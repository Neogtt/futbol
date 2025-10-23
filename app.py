import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import hashlib
from typing import Dict, List, Tuple, Optional, Set

TRUTHY_STRINGS = {
    "1",
    "true",
    "yes",
    "evet",
    "var",
    "✔",
    "✔️",
    "x",
    "✓",
    "✅",
    "doğru",
    "dogru",
    "active",
    "aktif",
    "acik",
    "açık",
    "open",
    "geldi",
}

COACH_ID_TO_NAME = {
    "1": "GOKHAN",
    "2": "SINAN",
    "3": "EMRE",
    "4": "TUGAY", 
}

MEMBERSHIP_STATUS_LABELS = {
    0: "Pasif",
    1: "Aktif",
    2: "Dondurulmuş",
}

MEMBERSHIP_STATUS_CODE_MAP = {
    "0": 0,
    "pasif": 0,
    "false": 0,
    "hayır": 0,
    "hayir": 0,
    "yok": 0,
    "no": 0,
    "inactive": 0,
    "kapali": 0,
    "kapalı": 0,
    "off": 0,
    "closed": 0,
    "✖": 0,
    "✖️": 0,
    "❌": 0,    
    "1": 1,
    "true": 1,
    "yes": 1,
    "evet": 1,
    "var": 1,
    "aktif": 1,
    "active": 1,
    "on": 1,
    "acik": 1,
    "açık": 1,
    "open": 1,
    "✔": 1,
    "✔️": 1,
    "✓": 1,
    "✅": 1,    
    "2": 2,
    "dondurulmuş": 2,
    "dondurulmus": 2,
    "donmus": 2,
    "frozen": 2,
    "askida": 2,
    "askıya": 2,
    "askiya": 2,
}

MEMBERSHIP_STATUS_ACTIVE_CODES = {1, 2}

MEMBERSHIP_STATUS_COLUMN_CANDIDATES = [
    "aktif",
    "üyelik durumu",
    "uyelik durumu",
    "üyelik durum",
    "uyelik durum",
    "üyelikdurumu",
    "üyelikdurumu",
    "üyelik",
    "uyelik",
    "üyelik_durumu",
    "üyelik_durumu",
    "üyelik_status",
    "üyelik_status",
    "uye durumu",
    "uyedurumu",
    "üyedurumu",
    "durum",
    "status",
]

def _simplify_token(token: str) -> str:
    return (
        token.replace("ç", "c")
        .replace("ğ", "g")
        .replace("ı", "i")
        .replace("ö", "o")
        .replace("ş", "s")
        .replace("ü", "u")
    )

COACH_NAME_TO_ID = {
    _simplify_token(str(name).strip().lower()): coach_id
    for coach_id, name in COACH_ID_TO_NAME.items()
}

def _normalize_colname(name: str) -> str:
    s = _simplify_token(str(name)).lower().strip()
    s = s.replace("_", "").replace(" ", "")
    return s

CANONICAL_COLMAP = {
    "ogrenciid": "OgrenciID",
    "ogrenci": "OgrenciID",
    "id": "OgrenciID",
    "adsoyad": "AdSoyad",
    "adisoyadi": "AdSoyad",
    "adisoyad": "AdSoyad",
    "isim": "AdSoyad",
    "adi": "AdSoyad",
    "ad": "AdSoyad",
    "grup": "Grup",
    "sinif": "Grup",
    "sınıf": "Grup",
    "koc": "Koc",
    "kocadi": "Koc",
    "kocoach": "Koc",
    "kocisim": "Koc",
    "kocid": "KocID",
    "coachid": "KocID",
    "aktif": "Aktif",
    "active": "Aktif",
    "uyedurumu": "Aktif",
    "durum": "Aktif",
    "uyelikdurumu": "UyelikDurumu",
    "uyelikdurum": "UyelikDurumu",
    "uyelik": "UyelikDurumu",
    "status": "UyelikDurumu",
}

def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renames = {}
    for c in df.columns:
        key = _normalize_colname(c)
        if key in CANONICAL_COLMAP:
            renames[c] = CANONICAL_COLMAP[key]
    if renames:
        df = df.rename(columns=renames)
    return df

def _is_truthy(value: object) -> bool:
    token = str(value).strip().lower()
    if not token or token in {"nan", "none"}:
        return False
    simplified = _simplify_token(token)
    return token in TRUTHY_STRINGS or simplified in TRUTHY_STRINGS

def _normalize_coach_id(value: object) -> str:
    token = str(value).strip()
    if not token or token.lower() in {"nan", "none"}:
        return ""
    try:
        numeric = int(float(token))
        return str(numeric)
    except (TypeError, ValueError):
        return token

def _resolve_coach(value: object) -> Tuple[str, str]:
    token = str(value).strip()
    if not token or token.lower() in {"nan", "none"}:
        return "", ""
    normalized_id = _normalize_coach_id(token)
    if normalized_id in COACH_ID_TO_NAME:
        name = COACH_ID_TO_NAME[normalized_id]
        return name, normalized_id
    simplified = _simplify_token(token.lower())
    coach_id = COACH_NAME_TO_ID.get(simplified, "")
    if coach_id:
        name = COACH_ID_TO_NAME.get(coach_id, token)
        return name, coach_id
    return token, ""

@st.cache_data(show_spinner=False)
def load_students() -> pd.DataFrame:
    empty = pd.DataFrame(columns=[
        "OgrenciID", "AdSoyad", "Grup", "Koc", "Aktif", "UyelikDurumu", "UyelikDurumuKodu"
    ])
    try:
        sheet_key = st.secrets["sheet"].get("key", "1WogWAT7rt6MANHORr2gd5E787Q_Zo0KtfrQkU1Tazfk")
        worksheet_name = st.secrets["sheet"].get("students_worksheet", "Ogrenciler")
        gc = gspread.authorize(Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/spreadsheets"]))
        ws = gc.open_by_key(sheet_key).worksheet(worksheet_name)
        df = pd.DataFrame(ws.get_all_records())
    except Exception as e:
        st.error(f"Veri okunamadı: {e}")
        return empty
    
    df = _canonicalize_columns(df)
    if "Koc" not in df:
        df["Koc"] = ""
    if "KocID" not in df:
        df["KocID"] = ""
    
    resolved_names = []
    resolved_ids = []
    for _, row in df.iterrows():
        coach_name, coach_id = _resolve_coach(row["Koc"])
        resolved_names.append(coach_name)
        resolved_ids.append(coach_id)
    
    df["Koc"] = resolved_names
    df["KocID"] = resolved_ids

    # Aktif öğrenci filtresi
    status_col = df["UyelikDurumu"].map(lambda x: x == 1)
    df = df[status_col]

    return df

@st.cache_data(show_spinner=False)
def get_students_for_coach(username: str) -> pd.DataFrame:
    df = load_students()
    if df.empty:
        return df
    username_clean = username.strip().lower()
    username_simple = _simplify_token(username_clean)
    
    mask = df["Koc"].apply(lambda x: x.strip().lower() == username_clean or _simplify_token(x.strip().lower()) == username_simple)
    df = df[mask]
    return df

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
    
    users = get_all_users()
    if not users:
        st.warning("Kullanıcı listesi boş görünüyor.")
    
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
