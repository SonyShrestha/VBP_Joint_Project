import streamlit as st
import os
import base64

# Import your pages
from pages.home import home 
from pages.OCR_invoice import ocr_invoice
from pages.product_perishability import product_perishability
from pages.cust_purchase_expected_expiry import cust_purchase_expected_expiry
from pages.dynamic_pricing_streamlit import dynamic_pricing_streamlit
from pages.closest_supermarket import closest_supermarket
from pages.sentiment_analysis import sentiment_analysis
from pages.time_series import show_feature5 as time_series
# from pages.food_recommender import food_recommender as food_recommender

# Set page configuration
st.set_page_config(page_title="SpicyBytes", layout="wide", initial_sidebar_state="collapsed")

# Define the path to the logo image
root_dir = os.path.abspath(os.path.join(os.getcwd()))
logo_path = os.path.join(root_dir, 'images', 'spicy_img1.jpg')

def main():
    # Add logo to the top right corner
    st.markdown(
        f"""
        <div class="top-left-logo">
            <img src="data:image/jpeg;base64,{base64.b64encode(open(logo_path, "rb").read()).decode()}" alt="Logo" width="300">
        </div>
        """,
        unsafe_allow_html=True
    )

    # Custom CSS to style buttons, position logo, and footer
    st.markdown(
        """
        <style>
        .stButton>button {
            font-size: 10;
            height: 100%;
            width: 100%;
            white-space: nowrap; /* Prevents text from wrapping */
            overflow: hidden; /* Keeps text from overflowing */
            text-overflow: ellipsis; /* Adds an ellipsis if text overflows */
            margin-bottom: 0; /* Removes space between buttons */
            padding: 1; /* Removes padding around buttons */
        }
        .top-right-logo {
            position: absolute;
            top: 10px;
            right: 10px;
            width: 100px; /* Adjust the size of the logo as needed */
        }
        .footer {
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            background-color: white;
            color: black;
            text-align: center;
            padding: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Create a single row of columns with equal width
    cols = st.columns(8)  # Creates 8 equally spaced columns (one less for Home)

    # Dictionary to map button names to functions
    pages = {
        'OCR': ocr_invoice,
        'Product Perishability': product_perishability,
        'Customer Inventory': cust_purchase_expected_expiry,
        'Dynamic Pricing': dynamic_pricing_streamlit,
        'Closest Supermarket': closest_supermarket,
        'Food Recommendation': sentiment_analysis,
        'Sentiment Analysis': sentiment_analysis,
        'Time Series Analysis': time_series
    }

    if 'page' not in st.session_state:
        st.session_state['page'] = 'Home'  # Default page

    # Generate buttons and assign functions
    for i, (title, func) in enumerate(pages.items()):
        with cols[i]:
            if st.button(title):
                st.session_state['page'] = title

    # Display selected page
    if 'page' in st.session_state and st.session_state['page'] in pages:
        pages[st.session_state['page']]()
    else:
        home()

    # Copyright footer
    st.markdown(
        """
        <div class="footer">
            &copy; 2024 SpicyBytes. All Rights Reserved.
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
