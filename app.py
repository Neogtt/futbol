from __future__ import annotations

import os, time, sqlite3, requests, io, zipfile, json, re
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, TYPE_CHECKING
from xml.sax.saxutils import escape
import numbers
import streamlit as st

try:
    from streamlit.runtime.secrets import StreamlitSecretNotFoundError
except Exception:  # pragma: no cover - optional dependency
    class StreamlitSecretNotFoundError(Exception):
        """Fallback when Streamlit's secret error type isn't available."""

        pass
import pandas as pd
from xml.etree import ElementTree as ET



try:
    from google.oauth2.service_account import Credentials as GoogleCredentials
except ImportError:  # pragma: no cover - optional dependency
    GoogleCredentials = None  # type: ignore[assignment]

try:
    import gspread
except ImportError:  # pragma: no cover - optional dependency
    gspread = None  # type: ignore[assignment]


if TYPE_CHECKING:  # pragma: no cover - typing helper
    import gspread as gspread_types

try:
    from gspread import WorksheetNotFound
except (ImportError, ModuleNotFoundError):  # pragma: no cover - optional dependency
    class WorksheetNotFound(Exception):
        """Fallback when gspread isn't installed."""

        pass

# ---------------------------
# Config & Secrets
# ---------------------------
st.set_page_config(page_title="Futbol Okulu • Tahsilat & WhatsApp", layout="wide")

def _get_secret(name: str, default: str = "") -> str:
    try:
        return st.secrets[name]
    except (KeyError, FileNotFoundError, StreamlitSecretNotFoundError):
        return os.getenv(name, default)


WHATSAPP_TOKEN = _get_secret("WHATSAPP_TOKEN")
WABA_PHONE_NUMBER_ID = _get_secret("WABA_PHONE_NUMBER_ID")  # e.g. "1234567890"
GRAPH_BASE = "https://graph.facebook.com/v20.0"

DEFAULT_DB_PATH = "futbol_okulu.db"
DEFAULT_GOOGLE_SHEET_ID = _get_secret("GOOGLE_SHEET_ID", "")
if not DEFAULT_GOOGLE_SHEET_ID:
    DEFAULT_GOOGLE_SHEET_ID = "17-vOIoebsR7W7Bp83tbKZ7QOoC3oeUJ8"

TABLE_NAMES = ["students", "groups", "invoices", "payments", "msg_log"]

EXPECTED_IMPORT_COLUMNS: dict[str, list[str]] = {
    "students": [
        "id",
        "ad",
        "soyad",
        "veli_ad",
        "veli_tel",
        "takim",
        "dogum_tarihi",
        "aktif_mi",
        "uye_tipi",
    ],
    "groups": ["id", "ad"],
    "invoices": [
        "id",
        "student_id",
        "donem",
        "tutar",
        "son_odeme_tarihi",
        "durum",
    ],
    "payments": ["id", "invoice_id", "tarih", "tutar", "aciklama"],
    "msg_log": [
        "id",
        "phone",
        "template_name",
        "msg_type",
        "payload",
        "status",
        "ts",
    ],
}

if "DB_PATH" not in st.session_state:
    st.session_state.DB_PATH = DEFAULT_DB_PATH

if "google_sheet_id" not in st.session_state:
    st.session_state.google_sheet_id = DEFAULT_GOOGLE_SHEET_ID

if "local_excel_path" not in st.session_state:
    default_local_excel = os.path.abspath("futbol_okulu.xlsx")
    st.session_state.local_excel_path = default_local_excel

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
# Backup & Restore Helpers
# ---------------------------


def fetch_db_tables() -> dict[str, pd.DataFrame]:
    conn = get_conn()
    tables: dict[str, pd.DataFrame] = {}
    try:
        for table in TABLE_NAMES:
            try:
                tables[table] = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            except Exception:
                tables[table] = pd.DataFrame()
    finally:
        conn.close()
    return tables


def _df_to_gspread_values(df: pd.DataFrame) -> list[list[Any]]:
    values: list[list[Any]] = []
    headers = [str(col) for col in df.columns]
    if headers:
        values.append(headers)
    for record in df.itertuples(index=False, name=None):
        row_values: list[Any] = []
        for value in record:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                row_values.append("")
            else:
                row_values.append(value)
        values.append(row_values)
    return values


def _load_service_account_info() -> dict[str, Any] | None:
    candidate_keys = [
        "gcp_service_account",
        "service_account",
        "google_service_account",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_APPLICATION_CREDENTIALS_JSON",
    ]
    for key in candidate_keys:
        try:
            secret_value = st.secrets[key]
            if isinstance(secret_value, dict):
                return secret_value
            if isinstance(secret_value, str) and secret_value.strip():
                return json.loads(secret_value)
        except (KeyError, FileNotFoundError):
            pass
        env_value = os.getenv(key)
        if env_value and env_value.strip():
            try:
                return json.loads(env_value)
            except json.JSONDecodeError:
                continue
    return None


def _get_gspread_client() -> tuple[gspread.Client | None, str | None]:
    if gspread is None:
        return None, (
            "Google Sheets entegrasyonu için gspread paketi yüklenmemiş. "
            "requirements.txt dosyasına uygun şekilde kurulumu yapın."
        )
    if GoogleCredentials is None:
        return None, (
            "Google Sheets entegrasyonu için google-auth paketi yüklenmemiş. "
            "requirements.txt dosyasına uygun şekilde kurulumu yapın."
        )

    info = _load_service_account_info()
    if not info:
        return None, (
            "Google service account bilgisi bulunamadı. "
            "Streamlit secrets veya ortam değişkenlerinde kimlik bilgilerini tanımlayın."
        )
    try:
        credentials = GoogleCredentials.from_service_account_info(
            info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ],
        )
        client = gspread.authorize(credentials)
        return client, None
    except Exception as exc:  # pragma: no cover - harici servis
        return None, f"Google yetkilendirme hatası: {exc}"


def export_db_to_excel_bytes() -> bytes:
    """Return the entire veritabanı as an Excel workbook (bytes)."""
    sheets = fetch_db_tables()        

    def _write_excel(engine: str) -> bytes:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine=engine) as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        buffer.seek(0)
        return buffer.getvalue()

    def _column_letter(idx: int) -> str:
        letters = []
        while idx > 0:
            idx, remainder = divmod(idx - 1, 26)
            letters.append(chr(65 + remainder))
        return "".join(reversed(letters)) or "A"

    def _sanitize_sheet_name(name: str, position: int, used: set[str]) -> str:
        invalid_chars = {"\\", "/", "*", "[", "]", ":", "?"}
        sanitized = "".join(ch for ch in name if ch not in invalid_chars).strip()
        sanitized = sanitized or f"Sheet{position}"
        if len(sanitized) > 31:
            sanitized = sanitized[:31]
        candidate = sanitized
        suffix = 1
        while candidate in used:
            suffix_str = f"_{suffix}"
            candidate = sanitized[: 31 - len(suffix_str)] + suffix_str
            suffix += 1
        used.add(candidate)
        return candidate

    def _write_simple_xlsx() -> bytes:
        timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        buffer = io.BytesIO()
        sheet_entries: list[tuple[str, str]] = []
        used_names: set[str] = set()

        for idx, (original_name, df) in enumerate(sheets.items(), start=1):
            sheet_name = _sanitize_sheet_name(original_name, idx, used_names)
            rows: list[list[Any]] = []
            if list(df.columns):
                rows.append([str(col) for col in df.columns])
            for record in df.itertuples(index=False, name=None):
                row_values: list[Any] = []
                for value in record:
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        row_values.append("")
                    else:
                        row_values.append(value)
                rows.append(row_values)

            max_columns = max((len(r) for r in rows), default=0)
            max_rows = len(rows)
            if max_rows == 0:
                max_rows = 1
            if max_columns == 0:
                dimension = "A1"
            else:
                dimension = f"A1:{_column_letter(max_columns)}{max_rows}"

            cells_xml: list[str] = []
            for r_idx, row in enumerate(rows, start=1):
                cell_xml: list[str] = []
                for c_idx in range(1, max_columns + 1):
                    cell_ref = f"{_column_letter(c_idx)}{r_idx}"
                    try:
                        value = row[c_idx - 1]
                    except IndexError:
                        value = ""
                    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
                        cell_xml.append(f"<c r=\"{cell_ref}\"/>")
                        continue
                    if isinstance(value, numbers.Number) and not isinstance(value, bool):
                        cell_xml.append(
                            f"<c r=\"{cell_ref}\"><v>{value}</v></c>"
                        )
                        continue
                    if isinstance(value, bool):
                        cell_xml.append(
                            f"<c r=\"{cell_ref}\" t=\"b\"><v>{int(value)}</v></c>"
                        )
                        continue
                    text_value = str(value)
                    if text_value == "":
                        cell_xml.append(f"<c r=\"{cell_ref}\"/>")
                    else:
                        cell_xml.append(
                            "<c r=\"{ref}\" t=\"inlineStr\"><is><t xml:space=\"preserve\">{text}</t></is></c>".format(
                                ref=cell_ref,
                                text=escape(text_value),
                            )
                        )
                cells_xml.append(
                    f"<row r=\"{r_idx}\">{''.join(cell_xml)}</row>"
                )

            sheet_xml = (
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\""
                " xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
                f"<dimension ref=\"{dimension}\"/>"
                "<sheetViews><sheetView workbookViewId=\"0\"/></sheetViews>"
                "<sheetFormatPr defaultRowHeight=\"15\"/>"
                f"<sheetData>{''.join(cells_xml)}</sheetData>"
                "</worksheet>"
            )
            sheet_entries.append((sheet_name, sheet_xml))

        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "[Content_Types].xml",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
                "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
                "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
                "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
                "<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>"
                + "".join(
                    f"<Override PartName=\"/xl/worksheets/sheet{idx}.xml\" "
                    "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
                    for idx in range(1, len(sheet_entries) + 1)
                )
                + "</Types>",
            )
            zf.writestr(
                "_rels/.rels",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
                "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>"
                "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>"
                "<Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>"
                "</Relationships>",
            )
            zf.writestr(
                "docProps/core.xml",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" "
                "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" "
                "xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                "<dc:creator>Futbol Okulu</dc:creator>"
                "<cp:lastModifiedBy>Futbol Okulu</cp:lastModifiedBy>"
                f"<dcterms:created xsi:type=\"dcterms:W3CDTF\">{timestamp}</dcterms:created>"
                f"<dcterms:modified xsi:type=\"dcterms:W3CDTF\">{timestamp}</dcterms:modified>"
                "</cp:coreProperties>",
            )
            zf.writestr(
                "docProps/app.xml",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" "
                "xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">"
                "<Application>Futbol Okulu</Application>"
                "</Properties>",
            )

            def _escape_sheet_attr(value: str) -> str:
                return escape(value, {'"': '&quot;'})

            workbook_sheets_xml = "".join(
                f"<sheet name=\"{_escape_sheet_attr(name)}\" sheetId=\"{idx}\" r:id=\"rId{idx}\"/>"
                for idx, (name, _) in enumerate(sheet_entries, start=1)
            )
            zf.writestr(
                "xl/workbook.xml",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
                "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
                "<bookViews><workbookView/></bookViews>"
                f"<sheets>{workbook_sheets_xml}</sheets>"
                "</workbook>",
            )

            workbook_rels_xml = "".join(
                f"<Relationship Id=\"rId{idx}\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet{idx}.xml\"/>"
                for idx in range(1, len(sheet_entries) + 1)
            )
            workbook_rels_xml += "<Relationship Id=\"rId{0}\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/>".format(len(sheet_entries) + 1)
            zf.writestr(
                "xl/_rels/workbook.xml.rels",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
                f"{workbook_rels_xml}"
                "</Relationships>",
            )

            zf.writestr(
                "xl/styles.xml",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
                "<fonts count=\"1\"><font><sz val=\"11\"/><color theme=\"1\"/><name val=\"Calibri\"/><family val=\"2\"/></font></fonts>"
                "<fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills>"
                "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>"
                "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
                "<cellXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/></cellXfs>"
                "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>"
                "</styleSheet>",
            )

            for idx, (_, sheet_xml) in enumerate(sheet_entries, start=1):
                zf.writestr(f"xl/worksheets/sheet{idx}.xml", sheet_xml)

        buffer.seek(0)
        return buffer.getvalue()


    try:
        return _write_excel("openpyxl")
    except ModuleNotFoundError:
        try:
            return _write_excel("xlsxwriter")
        except ModuleNotFoundError:
            return _write_simple_xlsx()


def _normalize_import_value(value, column: str):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None    
    if pd.isna(value):
        return None
    if column in {"id", "student_id", "invoice_id", "aktif_mi"}:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (float, int)) and not pd.isna(value):
            return int(value)
        value_str = str(value).strip()
        if value_str.isdigit():
            return int(value_str)
        raise ValueError(f"{column} sütunu için sayısal değer bekleniyor: {value}")
    if column in {"tutar"}:
        if isinstance(value, (float, int)):
            return float(value)
        value_str = str(value).replace(",", ".").strip()
        try:
            return float(value_str)
        except ValueError as exc:  # pragma: no cover - format guard
            raise ValueError(f"{column} sütunu için sayısal değer bekleniyor: {value}") from exc
    if column in {"dogum_tarihi", "son_odeme_tarihi", "tarih"}:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, str):
            return value.strip()
        raise ValueError(f"{column} sütunu için tarih değeri bekleniyor: {value}")
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value
    
def _reset_stream_position(handle: Any) -> None:
    if hasattr(handle, "seek"):
        try:
            handle.seek(0)
        except Exception:  # pragma: no cover - best effort
            pass


def _coerce_to_buffer(data_source: Any) -> io.BytesIO:
    if isinstance(data_source, (str, os.PathLike)):
        with open(data_source, "rb") as fh:
            return io.BytesIO(fh.read())
    if isinstance(data_source, bytes):
        return io.BytesIO(data_source)
    getvalue = getattr(data_source, "getvalue", None)
    if callable(getvalue):
        try:
            return io.BytesIO(getvalue())
        except TypeError:  # pragma: no cover - stream may not support getvalue
            pass
    read = getattr(data_source, "read", None)
    if callable(read):
        content = read()
        _reset_stream_position(data_source)
        return io.BytesIO(content)
    raise TypeError("Desteklenmeyen veri kaynağı")


def _column_index(column_ref: str) -> int:
    result = 0
    for char in column_ref.upper():
        if not ("A" <= char <= "Z"):
            continue
        result = result * 26 + (ord(char) - 64)
    return max(result, 1)


def _parse_cell_value(cell: ET.Element, namespaces: dict[str, str]) -> Any:
    cell_type = cell.attrib.get("t")
    value_element = cell.find("main:v", namespaces)
    if cell_type == "inlineStr":
        inline = cell.find("main:is", namespaces)
        text_element = inline.find("main:t", namespaces) if inline is not None else None
        if text_element is None:
            return None
        return text_element.text or ""
    if cell_type == "b":
        raw = value_element.text if value_element is not None else "0"
        try:
            return bool(int(str(raw or "0")))
        except ValueError:  # pragma: no cover - bozuk veri
            return False
    if value_element is None or (value_element.text or "").strip() == "":
        return None
    raw_text = value_element.text or ""
    try:
        as_float = float(raw_text)
        if as_float.is_integer():
            return int(as_float)
        return as_float
    except ValueError:
        return raw_text


def _read_simple_xlsx(data_source: Any) -> dict[str, pd.DataFrame]:
    buffer = _coerce_to_buffer(data_source)
    result: dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(buffer) as zf:
        workbook_xml = zf.read("xl/workbook.xml")
        workbook_tree = ET.fromstring(workbook_xml)
        namespaces = {
            "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
            "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        }
        rel_tree = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_ns = {
            "rel": "http://schemas.openxmlformats.org/package/2006/relationships"
        }
        relationships = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rel_tree.findall("rel:Relationship", rel_ns)
        }

        for sheet in workbook_tree.findall("main:sheets/main:sheet", namespaces):
            sheet_name = sheet.attrib.get("name", "Sheet")
            rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            if not rel_id:
                continue
            target = relationships.get(rel_id)
            if not target:
                continue
            target_path = target.lstrip("/")
            if not target_path.startswith("xl/"):
                target_path = f"xl/{target_path}"
            sheet_xml = ET.fromstring(zf.read(target_path))
            rows: list[list[Any]] = []
            for row in sheet_xml.findall("main:sheetData/main:row", namespaces):
                row_values: list[Any] = []
                current_col = 1
                for cell in row.findall("main:c", namespaces):
                    ref = cell.attrib.get("r", "")
                    match = re.match(r"([A-Za-z]+)", ref)
                    if match:
                        col_index = _column_index(match.group(1))
                    else:
                        col_index = current_col
                    while current_col < col_index:
                        row_values.append(None)
                        current_col += 1
                    row_values.append(_parse_cell_value(cell, namespaces))
                    current_col += 1
                rows.append(row_values)
            if not rows:
                result[sheet_name] = pd.DataFrame()
                continue
            max_columns = max((len(r) for r in rows), default=0)
            for r in rows:
                if len(r) < max_columns:
                    r.extend([None] * (max_columns - len(r)))
            header = [str(col).strip() if col is not None else "" for col in rows[0]]
            data_rows = []
            for row in rows[1:]:
                row = list(row[: len(header)])
                if len(row) < len(header):
                    row.extend([None] * (len(header) - len(row)))
                if any(cell not in (None, "") for cell in row):
                    data_rows.append(row)
            result[sheet_name] = pd.DataFrame(data_rows, columns=header)
    return result


def _read_excel_workbook(uploaded_file: Any) -> dict[str, pd.DataFrame]:
    try:
        _reset_stream_position(uploaded_file)
        return pd.read_excel(uploaded_file, sheet_name=None)
    except (ModuleNotFoundError, ImportError) as exc:
        message = str(exc)
        if "openpyxl" not in message:
            raise
        _reset_stream_position(uploaded_file)
        return _read_simple_xlsx(uploaded_file)




def import_db_from_excel(uploaded_file) -> tuple[bool, list[str]]:
    """İçe aktarma işlemi; başarı durumunu ve mesajları döner."""
    try:
        sheets = _read_excel_workbook(uploaded_file)
    except Exception as exc:  # pragma: no cover - kullanıcı girdisi
        return False, [f"Excel dosyası okunamadı: {exc}"]

    return import_db_from_frames(sheets)
    
def import_db_from_excel_path(path: str) -> tuple[bool, list[str]]:
    path = (path or "").strip()
    if not path:
        return False, ["Excel dosya yolu boş olamaz."]
    if not os.path.exists(path):
        return False, [f"Excel dosyası bulunamadı: {path}"]
    try:
        sheets = _read_excel_workbook(path)
    except Exception as exc:  # pragma: no cover - kullanıcı girdisi
        return False, [f"Excel dosyası okunamadı: {exc}"]
    return import_db_from_frames(sheets)



def import_db_from_frames(sheets: dict[str, pd.DataFrame]) -> tuple[bool, list[str]]:
    """İçe aktarma işlemi; başarı durumunu ve mesajları döner."""

    processed: list[str] = []
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=OFF")
        for table, columns in EXPECTED_IMPORT_COLUMNS.items():
            df = sheets.get(table)
            if df is None:
                continue
            missing = [col for col in columns if col not in df.columns]
            if missing:
                raise ValueError(f"{table} sayfasında eksik sütunlar: {', '.join(missing)}")
            subset = df[columns].copy()
            cursor.execute(f"DELETE FROM {table}")
            try:
                cursor.execute("DELETE FROM sqlite_sequence WHERE name=?", (table,))
            except sqlite3.Error:
                pass
            if subset.empty:
                processed.append(f"{table}: 0 satır aktarıldı")
                continue
            rows = []
            for row in subset.itertuples(index=False, name=None):
                cleaned = []
                for col_name, cell in zip(columns, row):
                    cleaned.append(_normalize_import_value(cell, col_name))
                rows.append(tuple(cleaned))
            placeholders = ",".join(["?"] * len(columns))
            column_sql = ",".join(columns)
            cursor.executemany(
                f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
                rows,
            )
            processed.append(f"{table}: {len(rows)} satır aktarıldı")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return False, [str(exc)]
    finally:
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
        except sqlite3.Error:
            pass
        conn.close()

    return True, processed if processed else ["Excel dosyasında beklenen sayfalar bulunamadı"]


def import_db_from_google_sheet(sheet_id: str) -> tuple[bool, list[str]]:
    sheet_id = (sheet_id or "").strip()
    if not sheet_id:
        return False, ["Google Sheet ID boş olamaz."]
    client, error = _get_gspread_client()
    if error:
        return False, [error]
    try:
        spreadsheet = client.open_by_key(sheet_id)
    except Exception as exc:  # pragma: no cover - harici servis
        return False, [f"Google Sheet açılamadı: {exc}"]

    sheets: dict[str, pd.DataFrame] = {}
    for table in TABLE_NAMES:
        try:
            worksheet = spreadsheet.worksheet(table)
        except WorksheetNotFound:
            
            continue
        values = worksheet.get_all_values()
        if not values:
            sheets[table] = pd.DataFrame(columns=EXPECTED_IMPORT_COLUMNS[table])
            continue
        header, *rows = values
        if not header:
            sheets[table] = pd.DataFrame(columns=EXPECTED_IMPORT_COLUMNS[table])
            continue
        df = pd.DataFrame(rows, columns=header)
        df = df.replace("", pd.NA)
        sheets[table] = df

    if not sheets:
        return False, ["Çalışma kitabında beklenen tablo adları bulunamadı."]

    return import_db_from_frames(sheets)


def export_db_to_google_sheet(sheet_id: str) -> tuple[bool, str]:
    sheet_id = (sheet_id or "").strip()
    if not sheet_id:
        return False, "Google Sheet ID boş olamaz."
    client, error = _get_gspread_client()
    if error:
        return False, error
    try:
        spreadsheet = client.open_by_key(sheet_id)
    except Exception as exc:  # pragma: no cover - harici servis
        return False, f"Google Sheet açılamadı: {exc}"

    tables = fetch_db_tables()
    for table, df in tables.items():
        try:
            worksheet = spreadsheet.worksheet(table)
        except WorksheetNotFound:
            rows = max(len(df) + 1, 1)
            cols = max(len(df.columns), 1)
            worksheet = spreadsheet.add_worksheet(title=table, rows=str(rows), cols=str(cols))
        values = _df_to_gspread_values(df)
        if not values:
            values = [[""]]
        row_count = len(values)
        col_count = max((len(row) for row in values), default=1)
        try:
            worksheet.clear()
            worksheet.resize(rows=row_count, cols=col_count)
            worksheet.update(values)
        except Exception as exc:  # pragma: no cover - harici servis
            return False, f"{table} sayfası yazılamadı: {exc}"

    return True, "Veritabanı Google Sheets'e aktarıldı."


def export_db_to_excel_file(path: str) -> tuple[bool, str]:
    path = (path or "").strip()
    if not path:
        return False, "Excel dosya yolu boş olamaz."
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
        except OSError as exc:  # pragma: no cover - dosya sistemi
            return False, f"Klasör oluşturulamadı: {exc}"
    try:
        with open(path, "wb") as fh:
            fh.write(export_db_to_excel_bytes())
    except OSError as exc:  # pragma: no cover - dosya sistemi
        return False, f"Excel dosyası yazılamadı: {exc}"
    return True, f"Veritabanı `{path}` dosyasına kaydedildi."


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

_WHATSAPP_ERROR_HINTS = {
    (131030, None): (
        "Numara Meta WhatsApp işletme hesabınızın izin verilen alıcılar listesinde değil. "
        "WhatsApp Manager → Phone numbers bölümünden \"Allowed recipients\" listesine "
        "veli numarasını ekleyin."
    ),
}


def _format_whatsapp_error(err: Dict[str, Any]) -> str | None:
    if not isinstance(err, dict):
        return None

    message = err.get("message")
    code = err.get("code")
    subcode = err.get("error_subcode")

    normalized_codes: List[tuple[int, Any]] = []

    if isinstance(code, numbers.Integral):
        norm_code = int(code)
        norm_subcode = int(subcode) if isinstance(subcode, numbers.Integral) else subcode
        normalized_codes.append((norm_code, norm_subcode))
        normalized_codes.append((norm_code, None))
    elif isinstance(code, str) and code.isdigit():
        norm_code = int(code)
        if isinstance(subcode, str) and subcode.isdigit():
            normalized_codes.append((norm_code, int(subcode)))
        normalized_codes.append((norm_code, None))

    for key in normalized_codes:
        hint = _WHATSAPP_ERROR_HINTS.get(key)
        if hint:
            if isinstance(message, str) and message.strip():
                return f"{message.strip()} — {hint}"
            return hint

    if isinstance(message, str) and message.strip():
        return message.strip()

    for alt_key in ("error_user_msg", "error_user_title", "details", "summary"):
        value = err.get(alt_key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None

def _response_error_message(resp: Dict[str, Any]) -> str:
    if resp.get("error"):
        return str(resp["error"])
    data = resp.get("data")
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            formatted = _format_whatsapp_error(err)
            if formatted:
                return formatted
            return str(err)
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
def _db_persistence_note() -> str:
    """Explain how the SQLite dosyası saklanıyor and warn about resets."""
    path = st.session_state.get("DB_PATH", DEFAULT_DB_PATH)
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    if os.path.exists(path):
        ts = datetime.fromtimestamp(os.path.getmtime(path))
        formatted = ts.strftime("%d %B %Y %H:%M")
        return (
            "Veriler bu sunucuda yerel bir SQLite dosyasında tutulur. "
            "Sunucu yeniden başlatılırsa ya da uygulama yeniden dağıtılırsa dosya sıfırlanabilir. "
            f"Mevcut dosya yolu: `{path}` (son güncelleme: {formatted})."
        )
    return (
        "Veriler yerel bir SQLite dosyasında saklanır. Sunucu yeniden başlarsa bu dosya yeniden "
        "oluşacağı için daha önceki kayıtlar kaybolabilir. Düzenli yedek almayı unutmayın."
    )


with st.sidebar:
    st.title("⚽ Futbol Okulu")
    st.caption("Ödeme Takip + WhatsApp")
    st.markdown("---")
    st.warning(_db_persistence_note())


    excel_bytes = export_db_to_excel_bytes()
    st.markdown("### 📁 Excel Yedekleme / Aktarma")
    st.download_button(
        "📤 Excel olarak dışa aktar",
        data=excel_bytes,
        file_name=f"futbol_okulu_{date.today().isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help="Tüm tablo verilerini Excel formatında indir.",
    )

        st.markdown("#### 💾 Yerel Excel Senkronizasyonu")
    local_excel_path = st.text_input(
        "Excel dosya yolu",
        value=st.session_state.get("local_excel_path", ""),
        help="Yerel diskteki Excel dosyasının tam yolunu girin.",
    )
    st.session_state.local_excel_path = local_excel_path
    col_local_export, col_local_import = st.columns(2)
    if col_local_export.button("📤 Dosyaya Kaydet", use_container_width=True):
        success, message = export_db_to_excel_file(local_excel_path)
        if success:
            st.success(message)
        else:
            st.error(message)
    if col_local_import.button("📥 Dosyadan Al", use_container_width=True):
        success, messages = import_db_from_excel_path(local_excel_path)
        status = "success" if success else "error"
        st.session_state["import_feedback"] = (status, messages)
        st.rerun()

    
    import_feedback = st.session_state.pop("import_feedback", None)
    if import_feedback:
        status, messages = import_feedback
        msg_text = "\n".join(messages)
        if status == "success":
            st.success(msg_text)
        else:
            st.error(msg_text)

    with st.form("excel_import_form"):
        st.caption("Excel içe aktarma mevcut verileri günceller. Lütfen önce yedek alın.")
        uploaded_excel = st.file_uploader("Excel (.xlsx) seç", type=["xlsx"], key="excel_import_file")
        import_submitted = st.form_submit_button("📥 Excel'den içe aktar")
        if import_submitted:
            if not uploaded_excel:
                st.warning("Lütfen içe aktarmak için bir Excel dosyası seçin.")
            else:
                success, messages = import_db_from_excel(uploaded_excel)
                status = "success" if success else "error"
                st.session_state["import_feedback"] = (status, messages)
                st.rerun()

    st.markdown("### ☁️ Google Sheets Senkronizasyonu")
    sheet_id_input = st.text_input(
        "Google Sheet ID",
        key="google_sheet_id",
        help="Google Sheets URL'sinde bulunan kimliği girin.",
    )
    st.caption(
        "Service account JSON bilgilerini `st.secrets` veya ortam değişkenlerinde tanımlayın."
    )
    col_gs_export, col_gs_import = st.columns(2)
    export_clicked = col_gs_export.button(
        "📤 Sheets'e Yedekle",
        use_container_width=True,
        disabled=not sheet_id_input.strip(),
    )
    import_clicked = col_gs_import.button(
        "📥 Sheets'ten İçe Aktar",
        use_container_width=True,
        disabled=not sheet_id_input.strip(),
    )

    if export_clicked:
        success, message = export_db_to_google_sheet(sheet_id_input)
        if success:
            st.success(message)
        else:
            st.error(message)

    if import_clicked:
        success, messages = import_db_from_google_sheet(sheet_id_input)
        status = "success" if success else "error"
        st.session_state["import_feedback"] = (status, messages)
        st.rerun()                
    
    
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

    df = df_invoices()
    overdue = df[(df["durum"] == "gecikti") & df["veli_tel"].notna()].copy()    
    if overdue.empty:
        st.info("Vadesi geçen aidat bulunmuyor.")
        st.session_state.whatsapp_overdue_selected = set()
        st.session_state.select_all_overdue = False
    else:
        if "whatsapp_overdue_selected" not in st.session_state:
            st.session_state.whatsapp_overdue_selected = set()

        current_ids = set(int(x) for x in overdue["id"].tolist())
        st.session_state.whatsapp_overdue_selected = {
            sid for sid in st.session_state.whatsapp_overdue_selected if sid in current_ids
        }

        all_selected = (
            len(st.session_state.whatsapp_overdue_selected) == len(current_ids)
            and len(current_ids) > 0
        )
        select_all = st.checkbox("Tümünü Seç", value=all_selected, key="select_all_overdue")
        if select_all and not all_selected:
            st.session_state.whatsapp_overdue_selected = set(current_ids)
        elif not select_all and all_selected:
            st.session_state.whatsapp_overdue_selected = set()

        display_df = overdue[[
            "id",
            "ad",
            "soyad",
            "donem",
            "tutar",
            "son_odeme_tarihi",
            "veli_tel",
]].copy()
        display_df.rename(
            columns={
                "id": "Fatura ID",
                "ad": "Ad",
                "soyad": "Soyad",
                "donem": "Dönem",
                "tutar": "Tutar",
                "son_odeme_tarihi": "Son Ödeme Tarihi",
                "veli_tel": "Veli Telefonu",
            },
            inplace=True,
        )
        display_df["Seç"] = display_df["Fatura ID"].apply(
            lambda x: int(x) in st.session_state.whatsapp_overdue_selected
        )

        edited_df = st.data_editor(
            display_df,
            column_config={
                "Seç": st.column_config.CheckboxColumn("Seç", default=False),
                "Tutar": st.column_config.NumberColumn("Tutar", format="%d TL"),
            },
            hide_index=True,
            disabled=[
                "Fatura ID",
                "Ad",
                "Soyad",
                "Dönem",
                "Tutar",
                "Son Ödeme Tarihi",
                "Veli Telefonu",
            ],
            use_container_width=True,
            key="overdue_editor",
        )

        selected_ids = [
            int(row["Fatura ID"])
            for _, row in edited_df.iterrows()
            if bool(row.get("Seç"))
        ]
        st.session_state.whatsapp_overdue_selected = set(selected_ids)
        st.session_state.select_all_overdue = (
            len(selected_ids) == len(current_ids) and len(current_ids) > 0
        )

        message_text = "Sayın velimiz Lütfen geciken ödemenizi en kısa sürede yapınız."
        st.markdown(f"**Gönderilecek Mesaj:** {message_text}")

        if st.button("Seçili Velilere Mesaj Gönder"):
            if not (WHATSAPP_TOKEN and WABA_PHONE_NUMBER_ID):
                st.error("WhatsApp ayarları eksik (token / phone number id).")
            else:
                if not selected_ids:
                    st.warning("Lütfen mesaj göndermek için listeden en az bir veli seçin.")
                    st.stop()
                phones = [
                    str(x).strip()
                    for x in overdue[overdue["id"].isin(selected_ids)]["veli_tel"].tolist()
                    if pd.notna(x) and str(x).strip()
                ]
                if not phones:
                    st.warning("Seçilen kayıtlar için geçerli veli telefonu bulunamadı.")
                    st.stop()
                sent = failed = 0
                error_msgs: List[str] = []
                for phone in phones:
                    resp = send_text(phone, message_text)
                    status = _response_status_label(resp)
                    log_msg(phone, "-", "text", message_text, status)
                    if status == "ok":
                        sent += 1
                    else:
                        failed += 1
                        error_msgs.append(f"{phone}: {_response_error_message(resp)}")
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
