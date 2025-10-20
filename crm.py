"""Ana giriş noktası: Rol bazlı yönlendirme sağlayan Streamlit uygulaması."""
from typing import Optional

import streamlit as st

from common import (
    USERS,
    authenticate_user,
    get_current_user,
    get_role_target,
    start_user_session,
)

st.set_page_config(page_title="Futbol Okulu Giriş", page_icon="⚽", layout="wide")


def _redirect_to_role_page(role: Optional[str]) -> None:
    target = get_role_target(role or "")
    if not target:
        st.error("Tanımsız rol için hedef sayfa bulunamadı. Lütfen yöneticinizle iletişime geçin.")
        return
    st.switch_page(target)


def show_logged_in_state() -> None:
    user = get_current_user()
    if not user:
        return
    st.success(f"Hoş geldiniz {user['display_name']}! Rolünüz: {user['role']}")
    st.write("İlgili sayfaya yönlendiriliyorsunuz...")
    _redirect_to_role_page(user.get("role"))


def show_login_form() -> None:
    st.title("Futbol Okulu Yönetim Girişi")
    st.caption("Lütfen kullanıcı adınız ve şifreniz ile giriş yapın.")

    with st.form("login_form"):
        username = st.text_input("Kullanıcı Adı")
        password = st.text_input("Şifre", type="password")
        submitted = st.form_submit_button("Giriş Yap")

    if submitted:
        user = authenticate_user(username, password)
        if user:
            start_user_session(user)
            st.experimental_rerun()
        else:
            st.error("Kullanıcı adı veya şifre hatalı.")

    st.subheader("Yetki Listesi")
    st.table(
        {
            "Kullanıcı": [u.display_name for u in USERS.values()],
            "Rol": [u.role for u in USERS.values()],
        }
    )


if st.session_state.get("authenticated"):
    show_logged_in_state()
else:
    show_login_form()
