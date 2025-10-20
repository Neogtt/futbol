"""Shared authentication and session utilities for Streamlit multi-page app."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class User:
    username: str
    password: str
    role: str
    display_name: str
    coach_code: Optional[str] = None


USERS: Dict[str, User] = {
    "crm_admin": User(username="crm_admin", password="crm123", role="crm", display_name="CRM Yöneticisi"),
    "coach_ahmet": User(
        username="coach_ahmet",
        password="coach123",
        role="coach",
        display_name="Koç Ahmet",
        coach_code="Ahmet",
    ),
}

ROLE_PAGES = {
    "crm": "pages/01_CRM.py",
    "coach": "pages/02_Koc.py",
}


def authenticate_user(username: str, password: str) -> Optional[User]:
    user = USERS.get(username)
    if user and password == user.password:
        return user
    return None


def start_user_session(user: User) -> None:
    st.session_state["authenticated"] = True
    st.session_state["username"] = user.username
    st.session_state["role"] = user.role
    st.session_state["display_name"] = user.display_name
    if user.coach_code:
        st.session_state["coach_code"] = user.coach_code


def end_user_session() -> None:
    for key in ["authenticated", "username", "role", "display_name"]:
        st.session_state.pop(key, None)
    st.session_state.pop("coach_code", None)


def get_current_user() -> Optional[Dict[str, str]]:
    if not st.session_state.get("authenticated"):
        return None
    return {
        "username": st.session_state.get("username"),
        "role": st.session_state.get("role"),
        "display_name": st.session_state.get("display_name", st.session_state.get("username")),
        "coach_code": st.session_state.get("coach_code"),
    }


def require_role(role: str) -> None:
    user = get_current_user()
    if user and user.get("role") == role:
        return
    st.warning("Bu sayfaya erişim yetkiniz yok. Giriş ekranına yönlendiriliyorsunuz.")
    st.switch_page("crm.py")
    st.stop()


def render_logout_button(label: str = "Oturumu Kapat") -> None:
    if st.sidebar.button(label, type="secondary"):
        end_user_session()
        st.experimental_rerun()


def get_role_target(role: str) -> Optional[str]:
    return ROLE_PAGES.get(role)


def ensure_dataframes_initialized() -> None:
    if "ogr" not in st.session_state:
        st.session_state["ogr"] = pd.DataFrame([
            {
                "ID": 1,
                "AdSoyad": "Demo Öğrenci",
                "Telefon": "0533",
                "Grup": "U10",
                "Seviye": "Başlangıç",
                "Koc": "Ahmet",
                "Baslangic": dt.date(2025, 9, 1),
                "UcretAylik": 1500,
                "SonOdeme": dt.date(2025, 10, 1),
                "Aktif": True,
                "AktifDurumu": "Aktif",
                "UyelikTercihi": 1,
            }
        ])
    if "yok" not in st.session_state:
        st.session_state["yok"] = pd.DataFrame(columns=["Tarih", "Grup", "OgrenciID", "AdSoyad", "Koc", "Katildi", "Not"])
    if "tah" not in st.session_state:
        st.session_state["tah"] = pd.DataFrame(columns=["Tarih", "OgrenciID", "AdSoyad", "Koc", "Tutar", "Aciklama"])
