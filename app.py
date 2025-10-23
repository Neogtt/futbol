import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Set

# =============================
# GENEL AYARLAR
# =============================
st.set_page_config(page_title="Yoklama – Koç Telefon Paneli", layout="wide")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_SHEET_KEY = "1WogWAT7rt6MANHORr2gd5E787Q_Zo0KtfrQkU1Tazfk"
DEFAULT_ATTENDANCE_WORKSHEET_NAME = "Yoklama"
DEFAULT_STUDENTS_WORKSHEET_NAME = "Ogrenciler"

# Koç ID ↔ İsim
COACH_ID_TO_NAME = {"1": "GOKHAN", "2": "SINAN", "3": "EMRE", "4": "TUGAY"}
COACH_NAME_TO_ID = {n.lower(): i for i, n in COACH_ID_TO_NAME.items()}

# Üyelik durum etiketleri ve aktif/frozen kodlar
MEMBERSHIP_STATUS_LABELS = {0: "Pasif", 1: "Aktif", 2: "Dondurulmuş"}
MEMBERSHIP_STATUS_ACTIVE_CODES = {1, 2}  # Sadece 1 olsun derseniz {1} yapın.

TRUTHY_STRINGS = {
    "1","true","yes","evet","var","✔","✔️","x","✓","✅","active","aktif","açık","acik","on","open","geldi"
}

ATTENDANCE_OPTIONS = ("✔️ VAR", "✖️ YOK")

# =============================
# YARDIMCI FONKSİYONLAR
# =============================
def _simplify_token(s: str) -> str:
    return (
        s.replace("ç","c").replace("ğ","g").replace("ı","i")
         .replace("ö","o").replace("ş","s").replace("ü","u")
    )

def _normalize_colname(name: str) -> str:
    s = _simplify_token(str(name)).lower().strip()
    return s.replace("_","").replace(" ","")

CANONICAL_COLMAP = {
    # ID
    "ogrenciid": "OgrenciID", "id": "OgrenciID", "ogrenci": "OgrenciID",

    # Ad Soyad
    "adsoyad": "AdSoyad", "adisoyadi": "AdSoyad", "adisoyad": "AdSoyad",
    "isim": "AdSoyad", "adi": "AdSoyad", "ad": "AdSoyad",

    # Grup
    "grup": "Grup", "sinif": "Grup", "sinifigrubu": "Grup",

    # Koç/KoçID
    "koc": "Koc", "kocadi": "Koc", "coach": "Koc", "coachname": "Koc",
    "kocid": "KocID", "coachid": "KocID",

    # Üyelik Durumu (sayısal/kelime)
    "uyelikdurumu": "UyelikDurumu", "uyelikdurum": "UyelikDurumu",
    "uyelik": "UyelikDurumu", "uyedurumu": "UyelikDurumu",
    "status": "UyelikDurumu", "durum": "UyelikDurumu",
    "üyelikdurumu": "UyelikDurumu",
}

def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
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
    if not token or token in {"nan","none"}: return False
    simple = _simplify_token(token)
    return token in TRUTHY_STRINGS or simple in TRUTHY_STRINGS

def _normalize_coach_pair(raw_value: object) -> Tuple[str, str]:
    """
    Girdi '2' ise -> ('SINAN','2'), 'SINAN' ise -> ('SINAN','2') gibi isim/ID çifti döndürür.
    Bilinmiyorsa ('ham değer','') döner.
    """
    t = str(raw_value).strip()
    if not t or t.lower() in {"nan","none"}:
        return "", ""
    # Sayısal ID mi?
    try:
        num = str(int(float(t)))
        if num in COACH_ID_TO_NAME:
            return COACH_ID_TO_NAME[num], num
    except:  # isim olabilir
        pass
    name = t.strip()
    id_guess = COACH_NAME_TO_ID.get(name.lower(),"")
    return (name if name else "", id_guess)

def _get_sheet_settings():
    s = st.secrets.get("sheet", {})
    key = s.get("key", DEFAULT_SHEET_KEY)
    ws_students = s.get("students_worksheet", DEFAULT_STUDENTS_WORKSHEET_NAME)
    ws_att = s.get("attendance_worksheet", s.get("worksheet", DEFAULT_ATTENDANCE_WORKSHEET_NAME))
    return key, ws_students, ws_att

@st.cache_resource(show_spinner=False)
def _gspread_client():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_data(show_spinner=False)
def load_users_from_secrets() -> Dict[str, Dict]:
    # secrets.toml: [credentials.X] password="..."
    creds = st.secrets.get("credentials", {})
    # Streamlit secrets nested TOML -> {'X': {'password': '...'}, ...}
    return {k: dict(v) for k, v in creds.items()}

# =============================
# VERİ OKUMA/YAZMA
# =============================
@st.cache_data(show_spinner=False)
def load_students() -> pd.DataFrame:
    empty = pd.DataFrame(columns=["OgrenciID","AdSoyad","Grup","Koc","KocID","UyelikDurumu","UyelikDurumuKodu"])
    try:
        key, ws_students, _ = _get_sheet_settings()
        gc = _gspread_client()
        ws = gc.open_by_key(key).worksheet(ws_students)
        df = pd.DataFrame(ws.get_all_records())
    except Exception as e:
        st.error(f"Öğrenciler okunamadı: {e}")
        return empty

    if df.empty:
        return empty

    df = _canonicalize_columns(df)

    # Eksik kolonları üret
    for c in ["OgrenciID","AdSoyad","Grup","Koc","KocID","UyelikDurumu"]:
        if c not in df: df[c] = ""

    # Koç isim/ID çöz
    names, ids = [], []
    for raw in df["Koc"]:
        name, cid = _normalize_coach_pair(raw)
        # KocID kolonu boşsa oradan da deneriz
        names.append(name)
        ids.append(cid)
    # Eğer KocID sütununda veri varsa boşları oradan doldur
    for i, cid in enumerate(ids):
        if not cid and str(df.loc[i,"KocID"]).strip():
            ids[i] = str(df.loc[i,"KocID"]).strip()
            if ids[i] in COACH_ID_TO_NAME:
                names[i] = COACH_ID_TO_NAME[ids[i]]

    df["Koc"] = names
    df["KocID"] = ids

    # Üyelik durumu sayısına çevir
    codes = []
    for v in df["UyelikDurumu"]:
        s = str(v).strip().lower()
        if s in {"1","aktif","active"}: codes.append(1)
        elif s in {"2","dondurulmus","dondurulmuş","frozen","askida","askıya","askiya"}: codes.append(2)
        elif s in {"0","pasif","inactive","kapali","kapalı","off"}: codes.append(0)
        else:
            # boş görünüyorsa aktif say
            try:
                n = int(float(s))
                n = n if n in (0,1,2) else 1
                codes.append(n)
            except:
                codes.append(1)
    df["UyelikDurumuKodu"] = codes
    df["UyelikDurumu"] = [MEMBERSHIP_STATUS_LABELS.get(c,"") for c in codes]

    # Aktif/frozen filtre
    df = df[df["UyelikDurumuKodu"].isin(MEMBERSHIP_STATUS_ACTIVE_CODES)].copy()

    # Temizlik
    df[["OgrenciID","AdSoyad","Grup","Koc","KocID"]] = (
        df[["OgrenciID","AdSoyad","Grup","Koc","KocID"]].astype(str).apply(lambda c: c.str.strip())
    )
    # Boş kimlik ve adları ele
    df = df[(df["OgrenciID"]!="") | (df["AdSoyad"]!="")].copy()

    return df

@st.cache_data(show_spinner=False)
def load_yoklama() -> pd.DataFrame:
    try:
        key, _, ws_att = _get_sheet_settings()
        gc = _gspread_client()
        ws = gc.open_by_key(key).worksheet(ws_att)
        df = pd.DataFrame(ws.get_all_records())
    except WorksheetNotFound:
        return pd.DataFrame(columns=["Tarih","Grup","OgrenciID","AdSoyad","Koc","Katildi","Not","Timestamp"])
    except Exception as e:
        st.error(f"Yoklama okunamadı: {e}")
        return pd.DataFrame(columns=["Tarih","Grup","OgrenciID","AdSoyad","Koc","Katildi","Not","Timestamp"])

    if df.empty:
        return pd.DataFrame(columns=["Tarih","Grup","OgrenciID","AdSoyad","Koc","Katildi","Not","Timestamp"])
    # normalize
    for c in ["Grup","OgrenciID","AdSoyad","Koc","Not"]:
        if c in df: df[c] = df[c].astype(str).str.strip()
    if "Katildi" in df:
        df["Katildi"] = df["Katildi"].astype(str).str.lower().isin({"true","1","evet","yes"})
    return df

def append_yoklama_rows(records: List[Dict]):
    key, _, ws_att = _get_sheet_settings()
    gc = _gspread_client()
    ws = gc.open_by_key(key).worksheet(ws_att)

    # Başlık yoksa oluştur
    all_vals = ws.get_all_values()
    if not all_vals:
        ws.update("A1:H1", [["Tarih","Grup","OgrenciID","AdSoyad","Koc","Katildi","Not","Timestamp"]])

    values = []
    for r in records:
        values.append([
            r.get("Tarih",""),
            r.get("Grup",""),
            r.get("OgrenciID",""),
            r.get("AdSoyad",""),
            r.get("Koc",""),
            "TRUE" if r.get("Katildi") else "FALSE",
            r.get("Not",""),
            r.get("Timestamp",""),
        ])
    if values:
        ws.append_rows(values, value_input_option="USER_ENTERED")

@st.cache_data(show_spinner=False)
def get_students_for_coach(username: str) -> pd.DataFrame:
    df = load_students()
    if df.empty:
        return df
    uname = str(username).strip()
    uname_lower = uname.lower()

    # Kullanıcı adı isimse doğrudan, ID ise tersinden eşle
    cand_ids = set()
    cand_names = set()

    # Eğer "SINAN" gibi isim
    cand_names.update({uname, uname_lower, _simplify_token(uname_lower)})
    if uname_lower in COACH_NAME_TO_ID:
        cid = COACH_NAME_TO_ID[uname_lower]
        cand_ids.add(cid)

    # Eğer "2" gibi ID ile girildiyse:
    try:
        num = str(int(float(uname)))
        cand_ids.add(num)
        if num in COACH_ID_TO_NAME:
            cand_names.add(COACH_ID_TO_NAME[num])
            cand_names.add(COACH_ID_TO_NAME[num].lower())
    except:
        pass

    mask = pd.Series([False]*len(df), index=df.index)
    if "Koc" in df:
        col = df["Koc"].astype(str)
        col_lower = col.str.lower()
        col_simple = col_lower.apply(_simplify_token)
        mask = mask | col_lower.isin({n.lower() for n in cand_names}) | col_simple.isin({ _simplify_token(n.lower()) for n in cand_names})
    if "KocID" in df:
        col = df["KocID"].astype(str).str.strip()
        col_lower = col.str.lower()
        mask = mask | col.isin(cand_ids) | col_lower.isin(cand_ids)

    if mask.any():
        out = df[mask].copy()
    else:
        out = df[df["Koc"].str.lower()==uname_lower].copy()

    if out.empty:
        return pd.DataFrame(columns=["OgrenciID","AdSoyad","Grup","Koc","KocID","UyelikDurumu","UyelikDurumuKodu"])
    out = out.drop_duplicates(subset=["OgrenciID","AdSoyad"], keep="first")
    return out[["OgrenciID","AdSoyad","Grup","Koc","KocID","UyelikDurumu","UyelikDurumuKodu"]].sort_values("AdSoyad")

# =============================
# GİRİŞ (Basit şifre)
# =============================
def load_user_dict() -> Dict[str, Dict]:
    return load_users_from_secrets()

def verify_password(users: Dict[str, Dict], username: str, password: str) -> bool:
    if username not in users:
        return False
    expected_plain = str(users[username].get("password",""))
    if expected_plain == "":
        # Parolasız mod: sadece kullanıcı adı yeterli (isterseniz False yapın)
        return True
    return str(password) == expected_plain

def login_view(users: Dict[str, Dict]) -> Tuple[str, bool]:
    st.markdown("### 👋 Koç Girişi")
    usernames = list(users.keys())
    username = st.selectbox("Kullanıcı adı", usernames if usernames else ["—"], index=0 if usernames else None)
    password = st.text_input("Şifre", type="password", key="password", placeholder="••••••••")
    ok = False
    if st.button("Giriş Yap", type="primary", use_container_width=True):
        if usernames and verify_password(users, str(username).strip(), password):
            st.session_state["auth_user"] = str(username).strip()
            ok = True
        else:
            st.error("Kullanıcı adı veya şifre hatalı.")
    return st.session_state.get("auth_user"), ok

# =============================
# ARAYÜZ – YOKLAMA
# =============================
def attendance_view(username: str):
    st.markdown(f"#### 👤 Oturum: **{username}**")

    # Yenile
    if st.button("🔄 Veriyi Yenile"):
        load_students.clear(); get_students_for_coach.clear(); load_yoklama.clear()
        st.experimental_rerun()

    # Debug expander
    with st.expander("🔎 Debug (geçici)"):
        df_all = load_students()
        st.write("Sütunlar:", list(df_all.columns))
        st.write("Koc uniq:", df_all.get("Koc", pd.Series(dtype=str)).unique().tolist() if not df_all.empty else "—")
        st.write("KocID uniq:", df_all.get("KocID", pd.Series(dtype=str)).unique().tolist() if not df_all.empty else "—")

    today = date.today()
    selected_date = st.date_input("Tarih", value=today, format="DD.MM.YYYY")
    date_str = selected_date.strftime("%d.%m.%Y")

    df_students = get_students_for_coach(username)
    if df_students.empty:
        st.info("Bu kullanıcıya atanmış **aktif** öğrenci bulunamadı. Lütfen Ogrenciler sekmesinde 'Koc' (isim veya ID) ve 'UYELIK DURUMU' (1/2) değerlerini kontrol edin.")
        return

    st.markdown("---")
    st.markdown("### ✅ Yoklama Listesi")
    present_map, note_map = {}, {}

    for row in df_students.itertuples(index=False):
        sid = str(row.OgrenciID)
        status = getattr(row, "UyelikDurumu", "")
        student_label = f"{row.AdSoyad} — (ID: {sid}) | Grup: {row.Grup}" + (f" | Durum: {status}" if status else "")
        radio_key = f"att_{date_str}_{sid}"
        note_key  = f"note_{date_str}_{sid}"

        if radio_key not in st.session_state:
            st.session_state[radio_key] = ATTENDANCE_OPTIONS[1]  # default YOK

        c1, c2 = st.columns([3,2])
        with c1: st.markdown(f"**{student_label}**")
        with c2:
            st.radio("Yoklama", ATTENDANCE_OPTIONS, key=radio_key, horizontal=True, label_visibility="collapsed")

        present_map[sid] = (st.session_state.get(radio_key) == ATTENDANCE_OPTIONS[0])
        if note_key not in st.session_state: st.session_state[note_key] = ""
        with st.expander("Not (opsiyonel)", expanded=False):
            st.text_input("Not", key=note_key)
        note_map[sid] = st.session_state[note_key]

    st.markdown("---")
    genel_not = st.text_input("Günün genel notu (opsiyonel)", value="")

    if st.button("💾 Yoklamayı Kaydet", type="primary", use_container_width=True):
        now_iso = datetime.now().isoformat(timespec="seconds")
        sid_to_name  = dict(zip(df_students["OgrenciID"].astype(str), df_students["AdSoyad"]))
        sid_to_group = dict(zip(df_students["OgrenciID"].astype(str), df_students["Grup"]))
        records = []
        for sid, present in present_map.items():
            records.append({
                "Tarih": date_str,
                "Grup": sid_to_group.get(sid, ""),
                "OgrenciID": sid,
                "AdSoyad": sid_to_name.get(sid, ""),
                "Koc": username,
                "Katildi": bool(present),
                "Not": note_map.get(sid) or genel_not,
                "Timestamp": now_iso,
            })
        try:
            append_yoklama_rows(records)
            load_yoklama.clear()
            st.success("Yoklama kaydedildi.")
        except Exception as e:
            st.error(f"Yazma hatası: {e}")

# =============================
# ANA AKIŞ
# =============================
def main():
    st.title("📋 Yoklama – Koç Telefon Paneli")
    st.caption("Öğrenciler 'Ogrenciler' sekmesinden okunur; yoklamalar 'Yoklama' sekmesine kaydedilir.")

    users = load_user_dict()
    if not users:
        st.warning("Kullanıcı listesi boş görünüyor. `.streamlit/secrets.toml` içindeki [credentials] bloklarını kontrol edin.")

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
