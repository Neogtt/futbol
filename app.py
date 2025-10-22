import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import hashlib
from typing import Dict, List, Tuple, Optional

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
    "uyelikdurumu",
    "üyelik",
    "uyelik",
    "üyelik_durumu",
    "uyelik_durumu",
    "üyelik_status",
    "uyelik_status",
    "uye durumu",
    "uyedurumu",
    "üyedurumu",
    "durum",
    "status",
]


ATTENDANCE_OPTIONS = (
    "✔️ VAR",
    "✖️ YOK",
)


def _simplify_token(token: str) -> str:
    return (
        token.replace("ç", "c")
        .replace("ğ", "g")
        .replace("ı", "i")
        .replace("ö", "o")
        .replace("ş", "s")
        .replace("ü", "u")
    )


def _is_truthy(value: object) -> bool:
    token = str(value).strip().lower()
    if not token or token in {"nan", "none"}:
        return False
    simplified = _simplify_token(token)
    return token in TRUTHY_STRINGS or simplified in TRUTHY_STRINGS


def _find_membership_status_column(df: pd.DataFrame) -> Optional[str]:
    lowered = {col.lower(): col for col in df.columns}
    for candidate in MEMBERSHIP_STATUS_COLUMN_CANDIDATES:
        if candidate in lowered:
            return lowered[candidate]
    if len(df.columns) >= 2:
        fallback_col = df.columns[1]
        series = df[fallback_col]
        meaningful = False
        for value in series:
            token = str(value).strip().lower()
            if not token or token in {"nan", "none"}:
                continue
            simplified = _simplify_token(token)
            if (
                token in MEMBERSHIP_STATUS_CODE_MAP
                or simplified in MEMBERSHIP_STATUS_CODE_MAP
                or token in TRUTHY_STRINGS
                or simplified in TRUTHY_STRINGS
            ):
                meaningful = True
                continue
            meaningful = False
            break
        if meaningful:
            return fallback_col
    return None


def _normalize_membership_status(value: object) -> Optional[int]:
    token = str(value).strip().lower()
    if not token or token in {"nan", "none"}:
        return None
    simplified = _simplify_token(token)
    if token in MEMBERSHIP_STATUS_CODE_MAP:
        return MEMBERSHIP_STATUS_CODE_MAP[token]
    if simplified in MEMBERSHIP_STATUS_CODE_MAP:
        return MEMBERSHIP_STATUS_CODE_MAP[simplified]
    if token in TRUTHY_STRINGS or simplified in TRUTHY_STRINGS:
        return 1
    return None

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
DEFAULT_SHEET_KEY = "1WogWAT7rt6MANHORr2gd5E787Q_Zo0KtfrQkU1Tazfk"
DEFAULT_ATTENDANCE_WORKSHEET_NAME = "Yoklama"  # Tek sayfa: "Tarih, Grup, OgrenciID, AdSoyad, Koc, Katildi, Not"
DEFAULT_STUDENTS_WORKSHEET_NAME = "Ogrenciler"  # Öğrenci listesi: "OgrenciID, AdSoyad, Grup, Koc, Aktif"


def get_sheet_settings() -> Tuple[str, str]:
    """st.secrets içinden yoklama sayfası kimliği ve adını okur, yoksa varsayılanı döner."""
    sheet_secrets = st.secrets.get("sheet", {})
    sheet_key = sheet_secrets.get("key", DEFAULT_SHEET_KEY)
    worksheet_name = sheet_secrets.get(
        "attendance_worksheet",
        sheet_secrets.get("worksheet", DEFAULT_ATTENDANCE_WORKSHEET_NAME),
    )
    return sheet_key, worksheet_name


def get_students_sheet_settings() -> Tuple[str, str]:
    """Öğrenci listesinin okunacağı sayfa ayarlarını döner."""
    sheet_secrets = st.secrets.get("sheet", {})
    sheet_key = sheet_secrets.get("students_key", sheet_secrets.get("key", DEFAULT_SHEET_KEY))
    worksheet_name = sheet_secrets.get("students_worksheet", DEFAULT_STUDENTS_WORKSHEET_NAME)
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
        df = load_students()
    except Exception:
        return []
    if df.empty or "Koc" not in df:
        return []
    return sorted({str(k).strip() for k in df["Koc"] if str(k).strip()})


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
    """Verify a user's password, supporting both hashed and plain entries."""    
    if username not in users:
        return False
    user_info = users[username]

    expected_hash = user_info.get("password_hash", "")
    expected_plain = user_info.get("password", "")

    # Parolasız mod: hem hash hem düz parola boşsa şifre kontrolü atlanır
    if not expected_hash and not expected_plain:
        return True

    if expected_hash:
        return sha256_hex(password) == expected_hash

    return password == expected_plain

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
    except WorksheetNotFound:
        st.error(
            "Google Sheet içinde beklenen çalışma sayfası bulunamadı. Lütfen sayfa adının doğru olduğunu ve "
            "belgede yer aldığını doğrulayın.\n\nDetay: "
            f"{worksheet_name}"
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
        df["Katildi"] = df["Katildi"].apply(_is_truthy)
    return df

@st.cache_data(show_spinner=False)
def load_students() -> pd.DataFrame:
    empty = pd.DataFrame(
        columns=[
            "OgrenciID",
            "AdSoyad",
            "Grup",
            "Koc",
            "Aktif",
            "UyelikDurumu",
            "UyelikDurumuKodu",
        ]
    )
    try:
        sheet_key, worksheet_name = get_students_sheet_settings()
        ws = open_ws_by_key(sheet_key, worksheet_name)
    except PermissionError as exc:
        st.error(
            "Öğrenci listesinin bulunduğu Google Sheet erişilemedi. Lütfen belgenin servis hesabı ile paylaşıldığından "
            "emin olun.\n\n"
            f"Detay: {exc}"
        )
        return empty
    except WorksheetNotFound:
        st.error(
            "Google Sheet içinde 'Ogrenciler' sayfası bulunamadı. Ayarlardan doğru sekme adını kullandığınızdan emin olun."
        )
        return empty
    except APIError as exc:
        message = str(exc)
        if "This operation is not supported for this document" in message:
            message += (
                "\n\nSeçilen kimlik bir Google E-Tablosu olmayabilir. ID'nin doğru olduğundan ve belgenin Google Sheet formatında"
                " olduğundan emin olun."
            )
        st.error(
            "Öğrenci listesini okurken bir hata oluştu. Google Sheet kimliğinizi ve erişim izinlerinizi kontrol edin.\n\n"
            f"Detay: {message}"
        )
        return empty
    except GSpreadException as exc:
        st.error(
            "Öğrenci listesini okurken bir hata oluştu. Google Sheet kimliğinizi ve erişim izinlerinizi kontrol edin.\n\n"
            f"Detay: {exc}"
        )
        return empty

    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        return empty

    normalize_cols = ["OgrenciID", "AdSoyad", "Grup", "Koc", "Aktif"]
    for col in normalize_cols:
        if col in df:
            df[col] = df[col].astype(str).str.strip()

    if "Aktif" in df:
        active_mask = df["Aktif"].apply(_is_truthy)
        df = df[active_mask].copy()
    
    status_col = _find_membership_status_column(df)
    if status_col:
        status_codes = df[status_col].apply(_normalize_membership_status)
        status_codes = pd.Series(status_codes, index=df.index, dtype="Int64")
    else:
        status_codes = pd.Series([1] * len(df), index=df.index, dtype="Int64")

    df["UyelikDurumuKodu"] = status_codes
    df["UyelikDurumu"] = (
        df["UyelikDurumuKodu"].map(MEMBERSHIP_STATUS_LABELS).fillna("")
    )

    if status_col:
        mask = df["UyelikDurumuKodu"].isin(MEMBERSHIP_STATUS_ACTIVE_CODES)
        mask = mask.fillna(False)
        df = df[mask].copy()

    if df.empty:
        return empty

    sid_series = df["OgrenciID"].astype(str).str.strip() if "OgrenciID" in df else pd.Series([""] * len(df), index=df.index)
    name_series = df["AdSoyad"].astype(str).str.strip() if "AdSoyad" in df else pd.Series([""] * len(df), index=df.index)
    df = df[(sid_series != "") | (name_series != "")].copy()

    for col in ["OgrenciID", "AdSoyad", "Grup", "Koc"]:
        if col not in df:
            df[col] = ""

    if "Katildi" in df:
        df["Katildi"] = df["Katildi"].apply(_is_truthy)
    else:
        df["Katildi"] = False
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
    except WorksheetNotFound as exc:
        raise RuntimeError(
            "Google Sheet içinde beklenen çalışma sayfası bulunamadı. Lütfen sayfa adını ve erişim izinlerini "
            f"kontrol edin.\n\nDetay: {worksheet_name}"
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
    df = load_students()
    if df.empty:
        return pd.DataFrame(columns=["OgrenciID", "AdSoyad", "Grup", "Koc"])

    df = df.copy()
    if "Koc" in df:
        df = df[df["Koc"].str.lower() == username.lower()].copy()

    if df.empty:
        return pd.DataFrame(columns=["OgrenciID", "AdSoyad", "Grup", "Koc"])

    # Aynı öğrenci birden fazla satırdaysa tekilleştir
    df = df.drop_duplicates(subset=["OgrenciID", "AdSoyad"], keep="first").copy()

    # Beklenen kolonlar eksikse oluştur
    for col in ["OgrenciID", "AdSoyad", "Grup", "Koc", "UyelikDurumu"]:
        if col not in df:
            df[col] = ""

    if "UyelikDurumu" in df:
        df["UyelikDurumu"] = df["UyelikDurumu"].astype(str).str.strip()
    else:
        df["UyelikDurumu"] = MEMBERSHIP_STATUS_LABELS[1]

    if "UyelikDurumuKodu" not in df:
        df["UyelikDurumuKodu"] = 1

    df.loc[:, ["OgrenciID", "AdSoyad", "Grup", "Koc"]] = (
        df.loc[:, ["OgrenciID", "AdSoyad", "Grup", "Koc"]]
        .astype(str)
        .apply(lambda col: col.str.strip())
    )

    return df[["OgrenciID", "AdSoyad", "Grup", "Koc", "UyelikDurumu", "UyelikDurumuKodu"]].sort_values("AdSoyad")

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
        st.info(
            "Bu kullanıcıya atanmış aktif öğrenci bulunamadı. Lütfen Google Sheet'teki 'Ogrenciler' sekmesinde koç atamasını "
            "ve 'Aktif' sütununu kontrol edin."
        )
        return

    # Aynı gün için önceden girilmiş kayıtları çek (prefill)
    df = load_yoklama()
    df_day = df[(df["Koc"].str.lower() == username.lower()) & (df["Tarih"].astype(str) == date_str)]
    pre = {str(r.OgrenciID): (bool(r.Katildi), str(r.Not) if str(r.Not) != "nan" else "") for r in df_day.itertuples(index=False)}

    st.markdown("---")
    st.markdown("### ✅ Yoklama Listesi")

    colA, colB = st.columns([1, 1])
    with colA:
        select_all = st.checkbox("Hepsini **VAR** (✔️) işaretle", value=False)
    with colB:
        clear_all = st.checkbox("Hepsini **YOK** (✖️) yap", value=False)

    if select_all and clear_all:
        st.warning("Lütfen yalnız birini seçin: Hepsini VAR **veya** Hepsini YOK.")

    present_map = {}
    note_map = {}

    attendance_options = ATTENDANCE_OPTIONS
    apply_select_all = select_all and not clear_all
    apply_clear_all = clear_all and not select_all


    for row in df_students.itertuples(index=False):
        sid = str(row.OgrenciID)
        status_label = getattr(row, "UyelikDurumu", "")
        status_label = str(status_label).strip()
        if status_label.lower() == "nan":
            status_label = ""
        status_suffix = f" | Durum: {status_label}" if status_label else ""
        student_label = f"{row.AdSoyad} — (ID: {sid}) | Grup: {row.Grup}{status_suffix}"
        status_code = getattr(row, "UyelikDurumuKodu", None)
        try:
            status_code_int = int(status_code)
        except (TypeError, ValueError):
            status_code_int = None
        default_present = pre.get(sid, (False, ""))[0]
        default_note = pre.get(sid, (False, ""))[1]
        if select_all:
            default_present = True
        if clear_all:
            default_present = False
        if status_code_int == 2 and sid not in pre:
            default_present = False
            
        radio_key = f"att_{date_str}_{sid}"
        note_key = f"note_{date_str}_{sid}"

        if radio_key not in st.session_state:
            st.session_state[radio_key] = attendance_options[0] if default_present else attendance_options[1]

        if apply_select_all:
            st.session_state[radio_key] = attendance_options[0]
        elif apply_clear_all:
            st.session_state[radio_key] = attendance_options[1]
        info_col, choice_col = st.columns([3, 2])
        with info_col:
            st.markdown(f"**{student_label}**")
        with choice_col:
            st.radio(
                "Yoklama durumu",
                attendance_options,
                key=radio_key,
                horizontal=True,
                label_visibility="collapsed",
            )
        present_map[sid] = st.session_state.get(radio_key) == attendance_options[0]

        if note_key not in st.session_state:
            st.session_state[note_key] = default_note

        with st.expander("Not (isteğe bağlı)", expanded=False):
            st.text_input("Not", key=note_key)
        note_map[sid] = st.session_state.get(note_key, "")

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
    st.caption(
        "Öğrenciler 'Ogrenciler' sekmesinden okunur; yoklamalar 'Yoklama' sekmesine Tarih, Grup, OgrenciID, AdSoyad, Koc, "
        "Katildi, Not başlıklarıyla kaydedilir."
    )

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
