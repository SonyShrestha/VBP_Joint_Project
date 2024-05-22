import streamlit as st
import pages as pg
from streamlit_navigation_bar import st_navbar
from streamlit_option_menu import option_menu
import os

# Set page config
st.set_page_config(page_title="My App", layout="wide", initial_sidebar_state="collapsed")

# Define pages and styles for navigation bar
pages = ["Product Perishability", "Expected Expiry of Customer Purchase", "Feature 3", "Feature 4", "Closest Supermarket"]
parent_dir = os.path.dirname(os.path.abspath(__file__))
styles = {
    "nav": {"background-color": "rgb(123, 209, 146)"},
    "div": {"max-width": "32rem"},
    "span": {"border-radius": "0.5rem", "color": "rgb(49, 51, 63)", "margin": "0 0.125rem", "padding": "0.4375rem 0.625rem"},
    "active": {"background-color": "rgba(255, 255, 255, 0.25)"},
    "hover": {"background-color": "rgba(255, 255, 255, 0.35)"}
}

# Navbar at the top of the page
page = st_navbar(pages, styles=styles)

# Side option menu to possibly handle additional controls or mirrored navigation
with st.sidebar:
    selected = option_menu(
        menu_title="Navigate",
        options=pages,
        default_index=0,
        icons=["calendar", "calendar-check", "gear", "gear-fill", "geo"]
    )

# Mapping functions to pages
functions = {
    "Product Perishability": pg.show_feature1,
    "Expected Expiry of Customer Purchase": pg.show_feature2,
    "Feature 3": pg.show_feature3,
    "Feature 4": pg.show_feature4,
    "Closest Supermarket": pg.closest_supermarket
}

# Execute function based on selected page from navbar or sidebar
if page in functions:
    functions[page]()
else:
    st.error("Selected page not found")
