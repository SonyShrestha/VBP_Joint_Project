import streamlit as st
import os 
import base64

root_dir = os.path.abspath(os.path.join(os.getcwd()))
logo_path = os.path.join(root_dir, 'images', 'homepage_image.png')

def home():
    st.markdown(f"""
                <div class="app">
            Your Personalized Grocery Management App
        </div>
        <div class="description">
            SpicyBytes is a grocery management platform aimed at reducing food wastage by offering a sustainable shopping and selling experience for groceries nearing their expiration date, rather than discarding them.
        </div>

         <div>
            <img src="data:image/jpeg;base64,{base64.b64encode(open(logo_path, "rb").read()).decode()}" alt="Logo" width="1400">
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown(
        """
        <style>
            .app {
                text-align: center;
                font-size: 40px;
                font-weight: bold;
                margin: 7px;
                color: green;
            }
            .description {
                text-align: center;
                margin: 7px;
                font-size: 17px;
                color: grey;
            }
        </style>
        """,
        unsafe_allow_html=True
    )