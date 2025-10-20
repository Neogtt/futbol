"""
FUTBOL OKULU — Streamlit (Py3.8+) — OTOMATIK BAŞLIK TESPİTİ
-----------------------------------------------------------
• Sayfanın adı ne olursa olsun çalışır (örn. 'students').
• Başlık satırı ilk satır değilse otomatik tespit eder (ilk 30 satırı tarar).
• Türkçe karakter/boşluk normalize + geniş alias + header tespiti → Öğrenci Listesi dolar.
• Üyelik kodları: 0=Kontenjan, 1=1 Aylık, 2=3 Aylık, 3=6 Aylık, 4=12 Aylık.
"""

import io
from datetime import date
import datetime as dt
from typing import Tuple, Optional, List
import sys

import pandas as pd
import streamlit as st

# Altair 5.4 currently relies on ``typing.TypedDict`` supporting the ``closed``
# keyword argument (PEP 705). Python 3.13 removed the provisional support that
# existed in previous versions, which causes ``TypeError: TypedDict takes no
# keyword arguments`` during ``import altair``.  We patch ``typing.TypedDict``
# to use the backport from ``typing_extensions`` when running on Python 3.13 so
# the import succeeds.
if sys.version_info >= (3, 13):
    import typing

    try:  # pragma: no cover - tiny compatibility shim
        class _AltairClosedTypedDict(typing.TypedDict, closed=True):
            pass
    except TypeError:
        try:
            from typing_extensions import TypedDict as _TypedDictBackport  # type: ignore
        except ModuleNotFoundError as exc:  # pragma: no cover - defensive branch
            raise RuntimeError(
                "typing_extensions>=4.12 is required when running on Python 3.13"
            ) from exc

        typing.TypedDict = _TypedDictBackport  # type: ignore[attr-defined]
        
import altair as alt

from common import (
    ensure_dataframes_initialized,
    get_current_user,
    render_logout_button,
    require_role,
)

# ==========================
# Normalizasyon
# ==========================

TR_MAP = str.maketrans({
    "Ç":"c","ç":"c","Ğ":"g","ğ":"g","İ":"i","I":"i","ı":"i","Ö":"o","ö":"o","Ş":"s","ş":"s","Ü":"u","ü":"u"
})

def norm_key(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().translate(TR_MAP).lower()
    for ch in ["-", ".", "/", "\\", "|"]:
        s = s.replace(ch, " ")
    s = s.replace("_", " ")
    s = " ".join(s.split())
    return s

def resolve_membership_status(value) -> str:
    if pd.isna(value):
        return "Pasif"

    numeric_value: Optional[int] = None

    if isinstance(value, bool):
        return "Aktif" if value else "Pasif"

    if isinstance(value, (int, float)) and not pd.isna(value):
        numeric_value = int(value)
    elif isinstance(value, str):
        nk = norm_key(value)
        if "dondur" in nk:
            return "Dondurmuş"
        if any(word in nk for word in ["pasif", "inaktif", "iptal", "ayril"]):
            return "Pasif"
        if any(word in nk for word in ["aktif", "true", "evet", "var"]):
            return "Aktif"
        try:
            numeric_value = int(float(nk.replace(",", ".")))
        except ValueError:
            numeric_value = None

    if numeric_value is not None:
        if numeric_value in {2, 3}:
            return "Dondurmuş"
        return "Aktif" if numeric_value != 0 else "Pasif"

    return "Aktif" if bool(value) else "Pasif"

BASE_COLS = [
    "ID","AdSoyad","DogumTarihi","VeliAdSoyad","Telefon",
    "Grup","Seviye","Koc","Baslangic","UcretAylik","SonOdeme","Aktif","AktifDurumu",
    "UyelikTercihi","UyelikGunTercihi","UyelikYenilemeTercihi"
]

ALIAS_MAP = {
    "id": "ID",
    "adi soyadi": "AdSoyad", "ad soyad": "AdSoyad", "adi": "AdSoyad",
    "dogumt": "DogumTarihi", "dogum t": "DogumTarihi", "dogum tarihi": "DogumTarihi", "dogumtarihi": "DogumTarihi",
    "veliadsoyad": "VeliAdSoyad", "veli ad soyad": "VeliAdSoyad", "veliad": "VeliAdSoyad", "veliadsoy": "VeliAdSoyad",
    "telefon": "Telefon", "tel": "Telefon",
    "grup": "Grup",
    "seviye": "Seviye",
    "koc": "Koc", "koç": "Koc",
    "kayit tarihi": "Baslangic", "kayittarihi": "Baslangic", "kayıt tarihi": "Baslangic", "kayıttarihi": "Baslangic",
    "ucret aylik": "UcretAylik", "ucretaylik": "UcretAylik",
    "son odeme": "SonOdeme", "sonodeme": "SonOdeme",
    "aktif": "Aktif",
    "uyeliktercihi": "UyelikTercihi", "uyelik tercihi": "UyelikTercihi",
    "uyelikyenilemetarihi": "SonOdeme", "uyelik yenileme tarihi": "SonOdeme",
    "uyelikguntercihi": "UyelikGunTercihi", "uyelik gun tercihi": "UyelikGunTercihi",
    "uyelikyenilemetercihi": "UyelikYenilemeTercihi", "uyelik yenileme tercihi": "UyelikYenilemeTercihi",
    "uyelik durumu": "UyelikDurumu",    
}

HEADER_HINTS = {"adi","soyadi","telefon","grup","koc","kayit","uyelik","seviye"}


def detect_header_row(df: pd.DataFrame, scan_rows: int = 30) -> int:
    """İlk scan_rows satırı tarar; en çok header ipucu içeren satırı başlık kabul eder.
    Dönen değer: header satır index'i. Bulamazsa 0 döner.
    """
    best_row, best_score = 0, -1
    max_r = min(scan_rows, len(df))
    for r in range(max_r):
        row_vals = [norm_key(v) for v in df.iloc[r].tolist()]
        score = 0
        for v in row_vals:
            for hint in HEADER_HINTS:
                if hint in v:
                    score += 1
        if score > best_score:
            best_score, best_row = score, r
    return best_row


def apply_aliases(cols: List[str]) -> List[str]:
    renamed = []
    for c in cols:
        nk = norm_key(c)
        renamed.append(ALIAS_MAP.get(nk, c))
    return renamed


def normalize_students_df(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Başlığı tespit et
    hdr = detect_header_row(df)
    header_vals = df.iloc[hdr].tolist()
    # 2) Yeni başlıkları uygula ve üst satırları at
    df2 = df.iloc[hdr+1:].reset_index(drop=True).copy()
    df2.columns = header_vals
    # 3) Alias uygula
    df2 = df2.rename(columns={c: ALIAS_MAP.get(norm_key(c), c) for c in df2.columns})
    # 4) Eksik kolonları tamamla
    for c in BASE_COLS:
        if c not in df2.columns:
            df2[c] = None
    # Türler
    if "ID" in df2.columns:
        df2["ID"] = pd.to_numeric(df2["ID"], errors="coerce")
        if df2["ID"].isna().all():
            df2["ID"] = range(1, len(df2) + 1)
    else:
        df2["ID"] = range(1, len(df2) + 1)
    df2["ID"] = df2["ID"].fillna(0).astype(int)

    for c in ("DogumTarihi","Baslangic","SonOdeme"):
        df2[c] = df2.get(c).apply(coerce_date_value)

    df2["UcretAylik"] = pd.to_numeric(df2.get("UcretAylik", 0), errors="coerce").fillna(0)
    df2["Telefon"] = df2.get("Telefon","").astype(str)

    if "UyelikDurumu" in df2.columns:
        raw_status = df2["UyelikDurumu"]
    elif "AktifDurumu" in df2.columns:
        raw_status = df2["AktifDurumu"]
    elif "Aktif" in df2.columns:
        raw_status = df2["Aktif"]
    else:
        raw_status = pd.Series([True] * len(df2))
    durumlar = [resolve_membership_status(v) for v in raw_status]
    df2["AktifDurumu"] = durumlar
    df2["Aktif"] = [d == "Aktif" for d in durumlar]

    df2["UyelikTercihi"] = pd.to_numeric(df2.get("UyelikTercihi", 0), errors="coerce").fillna(0).astype(int)
    df2["UyelikTercihi"] = df2["UyelikTercihi"].clip(lower=0, upper=12)  # esneklik, sonra 0-4'e kırparız

    return df2[BASE_COLS]


# ==========================
# Excel Yükleme (header tespitli)
# ==========================

@st.cache_data(show_spinner=False)
def load_excel(excel_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))
    sheets = xls.sheet_names
    # 'Ogrenciler' tercih et, yoksa ilk sayfa
    sheet = "Ogrenciler" if "Ogrenciler" in sheets else (sheets[0] if sheets else None)
    if sheet is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), ""
    # Başlıksız (header=None) okuyup tespit yapacağız
    raw = xls.parse(sheet, header=None)
    ogr = normalize_students_df(raw)
    # Opsiyonel sayfalar
    yok = xls.parse("Yoklama") if "Yoklama" in sheets else pd.DataFrame(columns=["Tarih","Grup","OgrenciID","AdSoyad","Koc","Katildi","Not"])
    tah = xls.parse("Tahsilat") if "Tahsilat" in sheets else pd.DataFrame(columns=["Tarih","OgrenciID","AdSoyad","Koc","Tutar","Aciklama"])
    for df_, dcol in [(yok,"Tarih"), (tah,"Tarih")]:
        if not df_.empty and dcol in df_.columns:
            df_[dcol] = pd.to_datetime(df_[dcol], errors="coerce").dt.date
    return ogr, yok, tah, sheet


def write_excel(ogr: pd.DataFrame, yok: pd.DataFrame, tah: pd.DataFrame) -> bytes:
    buff = io.BytesIO()
    with pd.ExcelWriter(buff, engine="openpyxl") as w:
        ogr.to_excel(w, index=False, sheet_name="Ogrenciler")
        yok.to_excel(w, index=False, sheet_name="Yoklama")
        tah.to_excel(w, index=False, sheet_name="Tahsilat")
    buff.seek(0)
    return buff.read()


def coerce_date_value(value) -> Optional[date]:
    """Çeşitli tarih formatlarını `date` nesnesine çevir."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
        # Sayı olarak sadece yıl bilgisi girilmişse (örn. 2010)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not float(value).is_integer():
            # Excel seri numaraları veya ondalıklar için pandas'a bırak.
            pass
        else:
            year = int(value)
            if 1900 <= year <= 2100:
                return date(year, 1, 1)

    if isinstance(value, str):
        value_stripped = value.strip()
        if not value_stripped:
            return None
        if value_stripped.isdigit() and len(value_stripped) == 4:
            year = int(value_stripped)
            if 1900 <= year <= 2100:
                return date(year, 1, 1)

    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        return parsed.date()
    if isinstance(parsed, date):
        return parsed
    return None

def parse_date_str(value: str) -> Optional[date]:
    return coerce_date_value(value)



def date_to_str(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, pd.Timestamp):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    value = str(value).strip()
    if value.lower() in {"nan", "nat", "none"}:
        return ""
    return value


def to_int(value, default: int = 0) -> int:
    if value is None:
        return default
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return default
    return int(numeric)

def format_display_value(value) -> str:
    if isinstance(value, pd.Timestamp):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""
    return str(value)



# ==========================
# Yaş Hesaplama
# ==========================

def compute_age(birth_value, today: date) -> Optional[int]:
    """Doğum tarihinden bugünkü yaşını hesapla."""
    if birth_value is None:
        return None
    if isinstance(birth_value, float) and pd.isna(birth_value):
        return None
    if isinstance(birth_value, pd.Timestamp):
        birth_date = birth_value.date()
    elif isinstance(birth_value, date):
        birth_date = birth_value
    else:
        birth_date = coerce_date_value(birth_value)
    if birth_date is None:
        return None
    if birth_date > today:
        return None
    years = today.year - birth_date.year - (
        (today.month, today.day) < (birth_date.month, birth_date.day)
    )
    return max(int(years), 0)



# ==========================
# Uygulama
# ==========================

require_role("crm")

user_info = get_current_user()
sidebar_title = "⚽ Futbol Okulu — Otomatik Başlık"
st.sidebar.title(sidebar_title)
if user_info:
    st.sidebar.caption(f"Giriş yapan: **{user_info['display_name']}**")
render_logout_button()

uploaded = st.sidebar.file_uploader("Excel yükle (.xlsx)", type=["xlsx"])
if uploaded:
    ogr_yuklu, yok_yuklu, tah_yuklu, used_sheet = load_excel(uploaded.getvalue())
    st.session_state["ogr"] = ogr_yuklu
    st.session_state["yok"] = yok_yuklu
    st.session_state["tah"] = tah_yuklu
    st.sidebar.success(f"Yüklenen sayfa: {used_sheet}")
ensure_dataframes_initialized()

ogr = st.session_state["ogr"]
yok = st.session_state["yok"]
tah = st.session_state["tah"]

# ==========================
# Genel Bakış ve Menü
# ==========================

UYELIK_AY = {0:0, 1:1, 2:3, 3:6, 4:12}
UYELIK_LABELS = {
    0: "",
    1: "1 Aylık",
    2: "3 Aylık",
    3: "6 Aylık",
    4: "12 Aylık",
}

def add_months(d: date, months: int) -> Optional[date]:
    if pd.isna(d) or d is None or months is None:
        return None
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    mdays = [31, 29 if (y%4==0 and (y%100!=0 or y%400==0)) else 28, 31,30,31,30,31,31,30,31,30,31][m-1]
    day = min(d.day, mdays)
    return date(y, m, day)

_today = dt.date.today()

def build_expiry_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["Baslangic"] = pd.to_datetime(tmp.get("Baslangic"), errors="coerce").dt.date
    tmp["UyelikTercihi"] = pd.to_numeric(tmp.get("UyelikTercihi", 0), errors="coerce").fillna(0).astype(int)
    tmp["UyelikTercihi"] = tmp["UyelikTercihi"].clip(lower=0, upper=4)
    
    if "AktifDurumu" in tmp.columns:
        tmp["UyelikDurumu"] = tmp["AktifDurumu"].fillna("Pasif")
    elif "Aktif" in tmp.columns:
        tmp["UyelikDurumu"] = tmp["Aktif"].apply(resolve_membership_status)
    else:
        tmp["UyelikDurumu"] = "Aktif"

    expires, remain, sure_ay = [], [], []
    for _, r in tmp.iterrows():
        code = int(r.get("UyelikTercihi", 0))
        months = UYELIK_AY.get(code, 0)
        sure_ay.append(months)
        start = r.get("Baslangic")
        e = add_months(start, months) if months > 0 else None
        expires.append(e)
        remain.append(None if e is None else (e - _today).days)

    tmp["UyelikSuresiAy"] = sure_ay
    tmp["UyelikBitisTarihi"] = expires
    tmp["KalanGun"] = remain
    tmp["UyelikTercihiAd"] = tmp["UyelikTercihi"].map(UYELIK_LABELS).fillna("Belirtilmedi")    

    cols = [
        "ID","AdSoyad","Koc","Grup","UyelikDurumu","UyelikTercihi","UyelikTercihiAd",
        "Baslangic","UyelikSuresiAy","UyelikBitisTarihi","KalanGun","Telefon"
    ]
    for c in cols:
        if c not in tmp.columns:
            tmp[c] = None
    return tmp[cols].sort_values(["KalanGun"], ascending=True, na_position="last")

exp_df = build_expiry_df(ogr)

menu_secimleri = [
    "Genel Bakış Panosu",
    "Üye Yönetimi",
    "Tüm Üyelikler",
    "İstatistikler",    
    "Dışa Aktarım",
]

secim = st.sidebar.radio("Menü", menu_secimleri, index=0)

toplam_ogrenci = len(ogr)

if not ogr.empty:
    if "AktifDurumu" in ogr.columns:
        durum_serisi = ogr["AktifDurumu"].fillna("Pasif").apply(resolve_membership_status)
    else:
        durum_serisi = ogr.get("Aktif", pd.Series([True] * len(ogr))).apply(resolve_membership_status)
else:
    durum_serisi = pd.Series(dtype=object)

aktif_say = int((durum_serisi == "Aktif").sum())
dondurmus_say = int((durum_serisi == "Dondurmuş").sum())
pasif_say = int((durum_serisi == "Pasif").sum())

if not exp_df.empty and "KalanGun" in exp_df:
    kalan_gun_serisi = pd.to_numeric(exp_df["KalanGun"], errors="coerce")
else:
    kalan_gun_serisi = pd.Series(dtype="float64")

odeme_5gun_say = int(kalan_gun_serisi.between(0, 5, inclusive="both").sum()) if not kalan_gun_serisi.empty else 0
odeme_gecikmis_say = int((kalan_gun_serisi < 0).sum()) if not kalan_gun_serisi.empty else 0

yenileme_maske = kalan_gun_serisi.between(-5, 5, inclusive="both") if not kalan_gun_serisi.empty else pd.Series([], dtype=bool)
yenileme_penceresi = int(yenileme_maske.sum()) if not exp_df.empty else 0

yenileme_df = exp_df[yenileme_maske] if not exp_df.empty else exp_df

if secim == "Genel Bakış Panosu":
    st.header("Genel Bakış Panosu")
    c1, c2, c3 = st.columns(3)
    c1.metric("Toplam Öğrenci", toplam_ogrenci)
    c2.metric("Dondurmuş Öğrenci", dondurmus_say)
    c3.metric("Aktif Öğrenci", aktif_say)

    c4, c5, c6 = st.columns(3)
    c4.metric("Pasif Öğrenci", pasif_say)
    c5.metric("Ödeme Gününe 5 Gün Kalan", odeme_5gun_say)
    c6.metric("Ödemesi Gecikmiş Öğrenci", odeme_gecikmis_say)

    st.markdown("**Yenileme penceresindeki öğrenciler (yenilemeden 5 gün önce - 5 gün sonra)**")
    if not yenileme_df.empty:
        st.dataframe(yenileme_df, use_container_width=True, hide_index=True)
    else:
        st.info("Yenileme tarihine ±5 gün penceresinde öğrenci bulunmuyor.")


elif secim == "Üye Yönetimi":
    st.header("Üye Yönetimi")
    if ogr.empty:
        st.info("Henüz öğrenci bulunmuyor. Yeni bir öğrenci ekleyebilirsiniz.")

    tab_ekle, tab_yenile, tab_duzenle, tab_sil = st.tabs([
        "Yeni Üye Ekle",
        "Kayıt Yenileme",
        "Üyeyi Düzenle",
        "Üye Sil",
    ])

    with tab_ekle:
        ogr_df = st.session_state["ogr"]
        mevcut_id_serisi = pd.to_numeric(ogr_df.get("ID"), errors="coerce") if "ID" in ogr_df.columns else pd.Series(dtype=float)
        varsayilan_id = to_int(mevcut_id_serisi.max(), default=0) + 1
        with st.form("yeni_ogrenci_form"):
            col1, col2, col3 = st.columns(3)
            yeni_id = col1.number_input("ID", min_value=1, value=varsayilan_id, step=1)
            ad_soyad = col1.text_input("Ad Soyad")
            veli_ad_soyad = col1.text_input("Veli Ad Soyad")
            telefon = col1.text_input("Telefon")

            dogum_tarihi = col2.text_input("Doğum Tarihi (YYYY-AA-GG)")
            grup = col2.text_input("Grup")
            seviye = col2.text_input("Seviye")
            koc = col2.text_input("Koç")

            baslangic_tarihi = col3.text_input("Başlangıç Tarihi (YYYY-AA-GG)")
            son_odeme = col3.text_input("Son Ödeme Tarihi (YYYY-AA-GG)")
            ucret = col3.number_input("Ücret (Aylık)", min_value=0.0, step=100.0, value=0.0)

            uyelik_kodlari = list(UYELIK_LABELS.keys())
            uyelik_tercihi = col3.selectbox("Üyelik Tercihi", uyelik_kodlari, format_func=lambda k: UYELIK_LABELS.get(k, ""))
            uyelik_gun = col2.text_input("Üyelik Gün Tercihi")

            aktif_durum_options = ["Aktif", "Dondurmuş", "Pasif"]
            aktif_durumu = col3.selectbox("Aktif Durumu", aktif_durum_options, index=0)

            ekle_submit = st.form_submit_button("Öğrenciyi Ekle")

        if ekle_submit:
            mevcut_idler = set(pd.to_numeric(ogr_df.get("ID"), errors="coerce").dropna().astype(int))
            yeni_id_int = int(yeni_id)
            if yeni_id_int in mevcut_idler:
                st.error("Bu ID'ye sahip bir öğrenci zaten mevcut. Lütfen farklı bir ID girin.")
            else:
                dogum_dt = parse_date_str(dogum_tarihi)
                baslangic_dt = parse_date_str(baslangic_tarihi)
                son_odeme_dt = parse_date_str(son_odeme)
                yeni_kayit = {
                    "ID": yeni_id_int,
                    "AdSoyad": ad_soyad.strip(),
                    "DogumTarihi": dogum_dt,
                    "VeliAdSoyad": veli_ad_soyad.strip(),
                    "Telefon": telefon.strip(),
                    "Grup": grup.strip(),
                    "Seviye": seviye.strip(),
                    "Koc": koc.strip(),
                    "Baslangic": baslangic_dt,
                    "UcretAylik": float(ucret),
                    "SonOdeme": son_odeme_dt,
                    "Aktif": aktif_durumu == "Aktif",
                    "AktifDurumu": aktif_durumu,
                    "UyelikTercihi": int(uyelik_tercihi),
                    "UyelikGunTercihi": uyelik_gun.strip(),
                }
                for col in BASE_COLS:
                    yeni_kayit.setdefault(col, None)
                st.session_state["ogr"] = pd.concat([ogr_df, pd.DataFrame([yeni_kayit])], ignore_index=True)
                st.success("Öğrenci eklendi.")
                st.experimental_rerun()

    with tab_duzenle:
        ogr_df = st.session_state["ogr"]
        if ogr_df.empty:
            st.info("Düzenlenecek öğrenci bulunmuyor.")
        else:
            secenekler = list(ogr_df.index)
            secilen_indeks = st.selectbox(
                "Düzenlenecek öğrenciyi seçin",
                options=secenekler,
                format_func=lambda idx: f"{ogr_df.loc[idx, 'ID']} - {ogr_df.loc[idx, 'AdSoyad']}"
            )
            satir = ogr_df.loc[secilen_indeks]
            mevcut_uyelik = to_int(satir.get("UyelikTercihi"), default=0)
            mevcut_aktif_durum = satir.get("AktifDurumu") or ("Aktif" if bool(satir.get("Aktif", True)) else "Pasif")

            st.markdown("**Seçilen Üyenin Mevcut Bilgileri**")
            gorunum = {col: format_display_value(satir.get(col)) for col in BASE_COLS if col in satir}
            if gorunum:
                st.dataframe(pd.DataFrame([gorunum]), use_container_width=True, hide_index=True)            

            with st.form(f"duzenle_form_{secilen_indeks}"):
                col1, col2, col3 = st.columns(3)
                duzenle_id = col1.number_input("ID", min_value=1, value=to_int(satir.get("ID"), default=1), step=1)
                duzenle_ad = col1.text_input("Ad Soyad", value=str(satir.get("AdSoyad") or ""))
                duzenle_veli = col1.text_input("Veli Ad Soyad", value=str(satir.get("VeliAdSoyad") or ""))
                duzenle_tel = col1.text_input("Telefon", value=str(satir.get("Telefon") or ""))

                duzenle_dogum = col2.text_input("Doğum Tarihi (YYYY-AA-GG)", value=date_to_str(satir.get("DogumTarihi")))
                duzenle_grup = col2.text_input("Grup", value=str(satir.get("Grup") or ""))
                duzenle_seviye = col2.text_input("Seviye", value=str(satir.get("Seviye") or ""))
                duzenle_koc = col2.text_input("Koç", value=str(satir.get("Koc") or ""))

                duzenle_baslangic = col3.text_input("Başlangıç Tarihi (YYYY-AA-GG)", value=date_to_str(satir.get("Baslangic")))
                duzenle_son_odeme = col3.text_input("Son Ödeme Tarihi (YYYY-AA-GG)", value=date_to_str(satir.get("SonOdeme")))
                duzenle_ucret = col3.number_input("Ücret (Aylık)", min_value=0.0, step=100.0, value=float(pd.to_numeric(satir.get("UcretAylik"), errors="coerce") or 0.0))

                uyelik_tercihi_duzenle = col3.selectbox("Üyelik Tercihi", list(UYELIK_LABELS.keys()), index=list(UYELIK_LABELS.keys()).index(mevcut_uyelik) if mevcut_uyelik in UYELIK_LABELS else 0, format_func=lambda k: UYELIK_LABELS.get(k, ""))
                duzenle_uyelik_gun = col2.text_input("Üyelik Gün Tercihi", value=str(satir.get("UyelikGunTercihi") or ""))
                duzenle_uyelik_yenileme = col2.text_input("Üyelik Yenileme Tercihi", value=str(satir.get("UyelikYenilemeTercihi") or ""))

                aktif_durum_options = ["Aktif", "Dondurmuş", "Pasif"]
                aktif_index = aktif_durum_options.index(mevcut_aktif_durum) if mevcut_aktif_durum in aktif_durum_options else 0
                duzenle_aktif_durum = col3.selectbox("Aktif Durumu", aktif_durum_options, index=aktif_index)

                duzenle_submit = st.form_submit_button("Değişiklikleri Kaydet")

            if duzenle_submit:
                duzenle_id_int = int(duzenle_id)
                mevcut_idler = set(pd.to_numeric(ogr_df.get("ID"), errors="coerce").dropna().astype(int)) - {int(pd.to_numeric(satir.get("ID"), errors="coerce") or 0)}
                if duzenle_id_int in mevcut_idler:
                    st.error("Bu ID başka bir öğrenciye ait. Lütfen farklı bir ID seçin.")
                else:
                    guncel_df = ogr_df.copy()
                    guncel_df.loc[secilen_indeks, "ID"] = duzenle_id_int
                    guncel_df.loc[secilen_indeks, "AdSoyad"] = duzenle_ad.strip()
                    guncel_df.loc[secilen_indeks, "DogumTarihi"] = parse_date_str(duzenle_dogum)
                    guncel_df.loc[secilen_indeks, "VeliAdSoyad"] = duzenle_veli.strip()
                    guncel_df.loc[secilen_indeks, "Telefon"] = duzenle_tel.strip()
                    guncel_df.loc[secilen_indeks, "Grup"] = duzenle_grup.strip()
                    guncel_df.loc[secilen_indeks, "Seviye"] = duzenle_seviye.strip()
                    guncel_df.loc[secilen_indeks, "Koc"] = duzenle_koc.strip()
                    guncel_df.loc[secilen_indeks, "Baslangic"] = parse_date_str(duzenle_baslangic)
                    guncel_df.loc[secilen_indeks, "UcretAylik"] = float(duzenle_ucret)
                    guncel_df.loc[secilen_indeks, "SonOdeme"] = parse_date_str(duzenle_son_odeme)
                    guncel_df.loc[secilen_indeks, "AktifDurumu"] = duzenle_aktif_durum
                    guncel_df.loc[secilen_indeks, "Aktif"] = duzenle_aktif_durum == "Aktif"
                    guncel_df.loc[secilen_indeks, "UyelikTercihi"] = int(uyelik_tercihi_duzenle)
                    guncel_df.loc[secilen_indeks, "UyelikGunTercihi"] = duzenle_uyelik_gun.strip()
                    guncel_df.loc[secilen_indeks, "UyelikYenilemeTercihi"] = duzenle_uyelik_yenileme.strip()
                    st.session_state["ogr"] = guncel_df.reset_index(drop=True)
                    st.success("Öğrenci bilgileri güncellendi.")
                    st.experimental_rerun()

    with tab_yenile:
        ogr_df = st.session_state["ogr"]
        if ogr_df.empty:
            st.info("Yenileme yapabilecek öğrenci bulunmuyor.")
        else:
            ogr_sec_options = list(ogr_df.index)

            with st.form("kayit_yenile_form"):
                secilen_indeks = st.selectbox(
                    "Yenilenecek öğrenciyi seçin",
                    options=ogr_sec_options,
                    format_func=lambda idx: f"{ogr_df.loc[idx, 'ID']} - {ogr_df.loc[idx, 'AdSoyad']}",
                )

                satir = ogr_df.loc[secilen_indeks]
                mevcut_baslangic = coerce_date_value(satir.get("Baslangic"))
                mevcut_uyelik = to_int(satir.get("UyelikTercihi"), default=0)
                varsayilan_tarih = add_months(mevcut_baslangic, UYELIK_AY.get(mevcut_uyelik, 0))
                if varsayilan_tarih is None:
                    varsayilan_tarih = dt.date.today()

                yenileme_tarihi = st.date_input(
                    "Yenileme Tarihi",
                    value=varsayilan_tarih,
                    help="Yenilenecek dönemin başlangıç tarihini seçin.",
                )

                uyelik_sec_options = list(UYELIK_LABELS.keys())
                uyelik_index = uyelik_sec_options.index(mevcut_uyelik) if mevcut_uyelik in uyelik_sec_options else 0
                secilen_uyelik = st.selectbox(
                    "Üyelik Tercihi",
                    options=uyelik_sec_options,
                    index=uyelik_index,
                    format_func=lambda k: UYELIK_LABELS.get(k, ""),
                    help="Yeni üyelik süresini seçin.",
                )

                yenile_submit = st.form_submit_button("Kayıt Yenile")

            if yenile_submit:
                yenileme_ay = UYELIK_AY.get(int(secilen_uyelik), 0)
                yeni_son_odeme = add_months(yenileme_tarihi, yenileme_ay) if yenileme_ay else None

                guncel_df = ogr_df.copy()
                guncel_df.loc[secilen_indeks, "Baslangic"] = yenileme_tarihi
                guncel_df.loc[secilen_indeks, "UyelikTercihi"] = int(secilen_uyelik)
                guncel_df.loc[secilen_indeks, "SonOdeme"] = yeni_son_odeme
                guncel_df.loc[secilen_indeks, "UyelikYenilemeTercihi"] = yenileme_tarihi.isoformat()
                guncel_df.loc[secilen_indeks, "AktifDurumu"] = "Aktif"
                guncel_df.loc[secilen_indeks, "Aktif"] = True

                st.session_state["ogr"] = guncel_df.reset_index(drop=True)

                if yeni_son_odeme:
                    st.success(
                        f"{satir.get('AdSoyad')} için yenileme tamamlandı. Yeni son ödeme tarihi: {yeni_son_odeme.isoformat()}."
                    )
                else:
                    st.success(f"{satir.get('AdSoyad')} için yenileme tamamlandı.")

                st.experimental_rerun()


    with tab_sil:
        ogr_df = st.session_state["ogr"]
        if ogr_df.empty:
            st.info("Silinecek öğrenci bulunmuyor.")
        else:
            silinecekler = st.multiselect(
                "Silinecek öğrencileri seçin",
                options=list(ogr_df.index),
                format_func=lambda idx: f"{ogr_df.loc[idx, 'ID']} - {ogr_df.loc[idx, 'AdSoyad']}"
            )
            if st.button("Seçilen Öğrencileri Sil"):
                if not silinecekler:
                    st.warning("Silmek için en az bir öğrenci seçin.")
                else:
                    guncel_df = ogr_df.drop(index=silinecekler).reset_index(drop=True)
                    st.session_state["ogr"] = guncel_df
                    st.success("Seçilen öğrenciler silindi.")
                    st.experimental_rerun()


elif secim == "Tüm Üyelikler":
    st.header("Tüm Üyelikler")
    if exp_df.empty:
        st.info("Gösterilecek üyelik bulunamadı.")
    else:
        durum_options = ["Aktif", "Pasif", "Dondurmuş"]
        grup_degerleri = [g for g in exp_df.get("Grup", pd.Series(dtype=object)).dropna() if str(g).strip()]
        group_options = sorted(set(grup_degerleri), key=lambda g: str(g))
        preference_options = [UYELIK_LABELS[k] for k in sorted(UYELIK_LABELS.keys())]
        extra_preferences = sorted({p for p in exp_df.get("UyelikTercihiAd", pd.Series(dtype=object)).dropna() if p not in preference_options}, key=lambda p: str(p))
        preference_options.extend(extra_preferences)

        filt_col1, filt_col2, filt_col3 = st.columns(3)
        secilen_durumlar = filt_col1.multiselect(
            "Üyelik durumu", durum_options, placeholder="Seçiniz"
        )
        secilen_gruplar = filt_col2.multiselect(
            "Grup", group_options, placeholder="Seçiniz"
        )
        secilen_tercihler = filt_col3.multiselect(
            "Üyelik tercihi", preference_options, placeholder="Seçiniz"
        )

        filtreli_df = exp_df.copy()
        if secilen_durumlar:
            filtreli_df = filtreli_df[filtreli_df["UyelikDurumu"].isin(secilen_durumlar)]
        if secilen_gruplar:
            filtreli_df = filtreli_df[filtreli_df["Grup"].isin(secilen_gruplar)]
        if secilen_tercihler:
            filtreli_df = filtreli_df[filtreli_df["UyelikTercihiAd"].isin(secilen_tercihler)]

        st.subheader("Üyelik listesi")
        if filtreli_df.empty:
            st.warning("Seçtiğiniz filtrelere uygun üyelik bulunamadı.")
        else:
            st.dataframe(filtreli_df, use_container_width=True, hide_index=True)

        if not filtreli_df.empty and "KalanGun" in filtreli_df.columns:
            kalan_gun_serisi = pd.to_numeric(filtreli_df["KalanGun"], errors="coerce")
        else:
            kalan_gun_serisi = pd.Series(dtype="float64", index=filtreli_df.index)
        expiring_mask = kalan_gun_serisi.between(0, 5, inclusive="both") if not kalan_gun_serisi.empty else pd.Series([], dtype=bool)
        expired_mask = kalan_gun_serisi.between(-5, -1, inclusive="both") if not kalan_gun_serisi.empty else pd.Series([], dtype=bool)

        tab1, tab2 = st.tabs(["5 Gün İçinde Bitecek", "5 Gündür Geciken"])
        with tab1:
            st.subheader("Üyeliği bitmeye 5 gün kalanlar")
            if not filtreli_df.empty and expiring_mask.any():
                st.dataframe(filtreli_df[expiring_mask], use_container_width=True, hide_index=True)
            else:
                st.info("Seçili filtrelerde 5 gün içinde bitecek üyelik bulunamadı.")
        with tab2:
            st.subheader("Üyeliği bitişinin üzerinden 5 gün geçenler")
            if not filtreli_df.empty and expired_mask.any():
                st.dataframe(filtreli_df[expired_mask], use_container_width=True, hide_index=True)
            else:
                st.info("Seçili filtrelerde son 5 gün içinde süresi dolmuş üyelik bulunamadı.")

elif secim == "İstatistikler":
    st.header("İstatistikler")
    if ogr.empty:
        st.info("Henüz öğrenci bulunmuyor. Önce veri yükleyin veya öğrenci ekleyin.")
    else:
        yas_serisi = ogr.get("DogumTarihi", pd.Series(dtype=object)).apply(
            lambda v: compute_age(v, _today)
        )
        yas_serisi = pd.to_numeric(yas_serisi, errors="coerce")

        yas_gruplari = pd.cut(
            yas_serisi,
            bins=[0, 7, 10, 13, 16, 19, 200],
            labels=["0-6", "7-9", "10-12", "13-15", "16-18", "19+"],
            right=False,
        )
        yas_df = (
            yas_gruplari.value_counts(dropna=False)
            .rename_axis("YasGrubu")
            .reset_index(name="Sayi")
        )
        if not yas_df.empty:
            yas_df["YasGrubu"] = (
                yas_df["YasGrubu"].astype(str).replace({"nan": "Belirtilmedi"})
            )
            toplam = yas_df["Sayi"].sum()
            if toplam > 0:
                yas_df["Yuzde"] = (yas_df["Sayi"] / toplam * 100).round(1)

        uyelik_serisi = pd.to_numeric(
            ogr.get("UyelikTercihi", pd.Series(dtype=object)), errors="coerce"
        )
        uyelik_serisi = uyelik_serisi.fillna(0).clip(lower=0, upper=4).astype(int)
        uyelik_etiketleri = (
            uyelik_serisi.map(UYELIK_LABELS).replace({"": "Belirtilmedi"}).fillna("Belirtilmedi")
        )
        uyelik_df = (
            uyelik_etiketleri.value_counts()
            .rename_axis("UyelikTercihi")
            .reset_index(name="Sayi")
        )
        if not uyelik_df.empty:
            toplam_uyelik = uyelik_df["Sayi"].sum()
            if toplam_uyelik > 0:
                uyelik_df["Yuzde"] = (uyelik_df["Sayi"] / toplam_uyelik * 100).round(1)

        kol1, kol2 = st.columns(2)

        with kol1:
            st.subheader("Yaş Grupları Dağılımı")
            if yas_df.empty or yas_df["Sayi"].sum() == 0:
                st.info("Yaş grubu hesaplamak için geçerli doğum tarihi bulunamadı.")
            else:
                yas_chart = (
                    alt.Chart(yas_df)
                    .mark_arc()
                    .encode(
                        theta=alt.Theta(field="Sayi", type="quantitative"),
                        color=alt.Color(field="YasGrubu", type="nominal"),
                        tooltip=[
                            alt.Tooltip("YasGrubu:N", title="Yaş Grubu"),
                            alt.Tooltip("Sayi:Q", title="Öğrenci"),
                            alt.Tooltip("Yuzde:Q", title="Oran (%)"),
                        ],
                    )
                )
                st.altair_chart(yas_chart, use_container_width=True)
                st.dataframe(yas_df, use_container_width=True, hide_index=True)

        with kol2:
            st.subheader("Üyelik Tercihi Dağılımı")
            if uyelik_df.empty or uyelik_df["Sayi"].sum() == 0:
                st.info("Üyelik tercihi verisi bulunamadı.")
            else:
                uyelik_chart = (
                    alt.Chart(uyelik_df)
                    .mark_arc()
                    .encode(
                        theta=alt.Theta(field="Sayi", type="quantitative"),
                        color=alt.Color(field="UyelikTercihi", type="nominal"),
                        tooltip=[
                            alt.Tooltip("UyelikTercihi:N", title="Üyelik"),
                            alt.Tooltip("Sayi:Q", title="Öğrenci"),
                            alt.Tooltip("Yuzde:Q", title="Oran (%)"),
                        ],
                    )
                )
                st.altair_chart(uyelik_chart, use_container_width=True)
                st.dataframe(uyelik_df, use_container_width=True, hide_index=True)


elif secim == "Dışa Aktarım":
    st.header("Dışa Aktarım")
    excel_bytes = write_excel(ogr, yok, tah)
    st.download_button(
        "Excel'i indir",
        data=excel_bytes,
        file_name=f"futbol_okulu_{dt.date.today().isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.caption("Otomatik başlık tespiti aktif. Sheet adı: fark etmez (örn. 'students').")
