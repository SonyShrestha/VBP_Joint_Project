import streamlit as st
from streamlit_navigation_bar import st_navbar
from streamlit_option_menu import option_menu
import pages as pg
import os

st.set_page_config(initial_sidebar_state="collapsed")

pages = ["Feature 1", "Feature 2", "Feature 3", "Feature 4", "Feature 5"]
parent_dir = os.path.dirname(os.path.abspath(__file__))
styles = {
    "nav": {
        "background-color": "rgb(123, 209, 146)",
    },
    "div": {
        "max-width": "32rem",
    },
    "span": {
        "border-radius": "0.5rem",
        "color": "rgb(49, 51, 63)",
        "margin": "0 0.125rem",
        "padding": "0.4375rem 0.625rem",
    },
    "active": {
        "background-color": "rgba(255, 255, 255, 0.25)",
    },
    "hover": {
        "background-color": "rgba(255, 255, 255, 0.35)",
    },
}

page = st_navbar(
    pages,
    styles=styles
)

# Get selected page from sidebar
with st.sidebar:
    selected = option_menu(
        menu_title=None,
        options=["Feature 1", "Feature 2", "Feature 3", "Feature 4", "Feature 5"],
        default_index=0
    )

# Map functions to pages
functions = {
    "Feature 1": pg.show_feature1,
    "Feature 2": pg.show_feature2,
    "Feature 3": pg.show_feature3,
    "Feature 4": pg.show_feature4,
    "Feature 5": pg.show_feature5,
}

# Determine selected page
# if selected:
#     page= selected
# if page:
#     selected= page

# Execute selected function
go_to = functions.get(page)
if go_to:
    go_to()

# with st.sidebar:
#         selected = option_menu(
#             menu_title= None,  # required
#             options=["Feature 1", "Feature 2", "Feature 3", "Feature 4", "Feature 5"],  # required
#             # icons=["house", "book", "envelope"],  # optional
#             # menu_icon="cast",  # optional
#             default_index=0,  # optional
#         )

# functions = {
#     "Feature 1": pg.show_feature1,
#     "Feature 2": pg.show_feature2,
#     "Feature 3": pg.show_feature3,
#     "Feature 4": pg.show_feature4,
#     "Feature 5": pg.show_feature5,
# }
# go_to = functions.get(page)
# if go_to:
#     go_to()

# if selected=="Feature 1":
#         pg.show_feature1()
# elif selected=="Feature 2":
#         pg.show_feature2()
# elif selected=="Feature 3":
#         pg.show_feature3()
# elif selected=="Feature 4":
#         pg.show_feature4()
# elif selected=="Feature 5":
#         pg.show_feature5()
