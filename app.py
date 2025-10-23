# app.py
import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Set

# =============================
# GENEL AYARLAR
# =============================
st.set_page_config(page_title="Yoklama – Koç Telefon Paneli", layout="wide")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
DEFAULT_SHEET_KEY = "1WogWAT7rt6MANHORr2gd5E787Q_Zo0KtfrQkU1Tazfk"
DEFAULT_STUDENTS_WORKSHEET_NAME = "Ogrenciler"
DEFAULT_ATTENDANCE_WORKSHEET_NAME = "Yoklama"

# Koç ID ↔ İsim eşleşmesi
COACH_ID_TO_NAME = {"1": "GOKHAN", "2": "SINAN", "3": "EMRE", "4": "TUGAY"}
COACH_NAME_TO_ID = {name.lower(): cid for cid, name in COACH_ID_TO_NAME.items()}

MEMBERSHIP_STATUS_LABELS = {0: "Pasif", 1: "Aktif", 2: "Dondurulmuş"}
# Dondurulmuş (2) öğrencileri de listelesin istiyorsanız {1,2}; sadece Aktif için {1}
MEMBERSHIP_STATUS_ACTIVE_CODES = {1, 2}

TRUTHY_STRINGS = {
    "1","true","yes","evet","var","✔","✔️","x","✓","✅","active","aktif","açık","acik","on","open","geldi"
}
ATTENDANCE_OPTIONS = ("✔️ VAR", "✖️ YOK")

# =============================
# YARDIMCI FONKSİYONLAR
# =============================
def _simplify_token(s: str) -> str:
    return (
        str(s)
        .replace("ç","c").replace("ğ","g").replace("ı","i")
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
    "grup": "Grup", "sinif": "Grup", "sınıf": "Grup",
    # Koç
    "koc": "Koc", "kocadi": "Koc", "coach": "Koc", "coachname": "Koc",
    "kocid": "KocID", "coachid": "KocID",
    # Üyelik Durumu
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

def _normalize_coach_pair(raw_value: object) -> Tuple[str, str]:
    """
    '2' → ('SINAN','2'), 'SINAN' → ('SINAN','2'); bilinmiyorsa ('ham','')
    """
    t = str(raw_value).strip()
    if not t or t.lower() in {"nan","none"}:
        return "", ""
    # ID?
    try:
        num = str(int(float(t)))
        if num in COACH_ID_TO_NAME:
            return COACH_ID_TO_NAME[num], num
    except:
        pass
    # İsim?
    name = t.strip()
    cid = COACH_NAME_TO_ID.get(name.lower(),"")
    return (name if name else "", cid)

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
    # secrets.toml → [credentials.X] password="..."
    creds = st.secrets.get("credentials", {})
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
        names.append(name); ids.append(cid)

    # KocID sütunu doluysa boşları oradan tamamla
    for i, cid in enumerate(ids):
        if not cid:
            cid2 = str(df.loc[i,"KocID"]).strip()
            if cid2:
                ids[i] = cid2
                names[i] = COACH_ID_TO_NAME.get(cid2, names[i])

    df["Koc"] = names
    df["KocID"] = ids

    # Üyelik durumunu koda çevir
    codes = []
    for v in df["UyelikDurumu"]:
        s = _simplify_token(str(v)).lower().strip()
        if s in {"1","aktif","active"}: codes.append(1)
        elif s in {"2","dondurulmus","dondurulmuş","frozen","askida","askiya","askıya"}: codes.append(2)
        elif s in {"0","pasif","inactive","kapali","kapalı","off"}: codes.append(0)
        else:
            try:
                n = int(float(s)); codes.append(n if n in (0,1,2) else 1)
            except:
                codes.append(1)
    df["UyelikDurumuKodu"] = codes
    df["UyelikDurumu"] = [MEMBERSHIP_STATUS_LABELS.get(c,"") for c in codes]

    # Aktif/dondurulmuş filtre
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

    for c in ["Grup","OgrenciID","AdSoyad","Koc","Not"]:
        if c in df: df[c] = df[c].astype(str).str.strip()
    if "Katildi" in df:
        df["Katildi"] = df["Katildi"].astype(str).str.lower().isin({"true","1","evet","yes"})
    return df

def append_yoklama_rows(records: List[Dict]):
    key, _, ws_att = _get_sheet_settings()
    gc = _gspread_client()
    ws = gc.open_by_key(key).worksheet(ws_att)

    # Başlık yoksa yaz
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

    cand_ids: Set[str] = set()
    cand_names: Set[str] = {uname, uname_lower, _simplify_token(uname_lower)}
    if uname_lower in COACH_NAME_TO_ID:
        cand_ids.add(COACH_NAME_TO_ID[uname_lower])

    # Eğer kullanıcı adı ID ise
    try:
        num = str(int(float(uname)))
        cand_ids.add(num)
        if num in COACH_ID_TO_NAME:
            cand_names.add(COACH_ID_TO_NAME[num]); cand_names.add(COACH_ID_TO_NAME[num].lower())
    except:
        pass

    mask = pd.Series([False]*len(df), index=df.index)
    if "Koc" in df:
        col = df["Koc"].astype(str)
        col_lower = col.str.lower()
        col_simple = col_lower.apply(_simplify_token)
        mask = mask | col_lower.isin({n.lower() for n in cand_names}) | col_simple.isin({_simplify_token(n.lower()) for n in cand_names})
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
# GİRİŞ – Basit şifre (secrets)
# =============================
def load_user_dict() -> Dict[str, Dict]:
    users = load_users_from_secrets()
    if users:
        return users
    # secrets yoksa, sheet'teki koç isimlerinden parolasız mod
    df = load_students()
    names = sorted(set([n for n in df.get("Koc", pd.Series([], dtype=str)).astype(str).tolist() if n]))
    return {n: {"password": ""} for n in names}

def verify_password(users: Dict[str, Dict], username: str, password: str) -> bool:
    if username not in users:
        return False
    expected_plain = str(users[username].get("password",""))
    # Parola tanımlanmamışsa kullanıcı adı yeterli (parolasız mod)
    if expected_plain == "":
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
# ARAYÜZ – GRUP FİLTRELİ YOKLAMA
# =============================
def attendance_view(username: str):
    st.markdown(f"#### 👤 Oturum: **{username}**")

    # Yenile
    if st.button("🔄 Veriyi Yenile"):
        load_students.clear(); get_students_for_coach.clear(); load_yoklama.clear()
        st.experimental_rerun()

    # Koça ait öğrenciler
    df_students_full = get_students_for_coach(username)
    if df_students_full.empty:
        st.info("Bu kullanıcıya atanmış **aktif** öğrenci bulunamadı. Ogrenciler sekmesinde 'Koc' (isim veya ID) ve 'UyelikDurumu' (1/2) değerlerini kontrol edin.")
        return

    # Grup seçenekleri (benzersiz, boşları at)
    groups_all = sorted(g for g in pd.Series(df_students_full["Grup"]).fillna("").astype(str).str.strip().unique() if g)
    default_groups = st.session_state.get("selected_groups", groups_all)
    selected_groups = st.multiselect(
        "📚 Grup seçin (birden fazla seçebilirsiniz)",
        options=groups_all,
        default=default_groups,
        placeholder="Grup seçin…"
    )
    st.session_state["selected_groups"] = selected_groups

    # Seçime göre filtre
    df_students = df_students_full.copy()
    if selected_groups:
        df_students = df_students[df_students["Grup"].isin(selected_groups)].copy()

    if df_students.empty:
        st.warning("Seçtiğiniz gruplarda öğrenci bulunamadı.")
        return

    # Tarih
    today = date.today()
    selected_date = st.date_input("📅 Tarih", value=today, format="DD.MM.YYYY")
    date_str = selected_date.strftime("%d.%m.%Y")

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
            st.session_state[radio_key] = ATTENDANCE_OPTIONS[1]  # default: YOK

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
