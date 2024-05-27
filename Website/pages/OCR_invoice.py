from dotenv import load_dotenv
import streamlit as st
import os
import json
from PIL import Image as img
import google.generativeai as genai
import pandas as pd
import base64
import vertexai
from vertexai.preview.generative_models import GenerativeModel, Part, FinishReason, Image
import vertexai.preview.generative_models as generative_models

def generate(input, image):
  vertexai.init(project="earnest-sight-404409", location="us-central1")
  model = GenerativeModel(
    "gemini-1.5-pro-001",
  )
  responses = model.generate_content(
      [image, input],
      generation_config=generation_config,
      safety_settings= safety_config
  )

  return responses.text


generation_config = {
    "max_output_tokens": 8192,
    "temperature": 0.6,
    "top_p": 0.95,
}

safety_config = {
    generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_NONE,
    generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_NONE,
}

def input_image_details(uploaded_file):
  if uploaded_file is not None:
    bytes_data= uploaded_file.getvalue()

    image_parts= [
      {
        'mime_type': uploaded_file.type,
        'data': bytes_data
      }
    ]
    return image_parts
  else:
    raise FileNotFoundError("no uploaded file")

def convert_file_to_data(file_path, mime_type):
  if uploaded_file is not None:
    bytes_data= uploaded_file.getvalue()
  data= base64.b64encode(bytes_data)
  file_data= Part.from_data(data= base64.b64decode(data), mime_type=mime_type)
  return file_data  


load_dotenv()

st.set_page_config(page_title= "INVOICE-INFO EXTRACTOR")

st.header("OCR INVOICE DETAILS EXTRACTION")
# input= st.text_input("Input Prompt: ", key= "input")
uploaded_file= st.file_uploader("choose an image...", type=["jpg", "jpeg", "png"])
image=""
if uploaded_file is not None:
  image= img.open(uploaded_file)
  st.image(image, caption="Uploaded Image", use_column_width=True)

submit= st.button("EXTRACT DEATILS FROM THE INVOICE")

input_prompt= """
You are an expert in analyzing supermarket purchase invoices in different languages.
Translate and give the output in english if the invoice is not in english.
Output the following information from the invoice only in JSON format: 
invoice no., name of the supermarket, address of the supermarket, date of purchase,
name of the products with their Quantity, Unit price and  Amount, 
Total price, total number of products purchased and mode of payment

Example output : 
  {
    "Invoice no.": "4312-013-538383",
    "location": "CTRA. DE COLLBLANC 90 08028 BARCELONA",
    "supermarket_name": "MERCADONA",
    "Date": "30/03/2024",
    "products": [
        {"name": "PA BLANC FAMILIAR", "Unit price": "1,25", "quantity": "1", "amount": "1,25"},
        {"name": "LLET FRESCA SENCERA", "Unit price": "1,05", "quantity": "1", "amount": "1,05"},
        {"name": "PATATES XILI I LLIMA", "Unit price": "1,15", "quantity": "1", "amount": "1,15"},
        {"name": "QUARTER POSTERIOR", "Unit price": "4,48", "quantity": "1", "amount": "4,48"}
    ],
    "total_price": "9,51",
    "total_products": "5",
    "mode_of_payment": "TARGETA BANCARIA"
  }
"""

if submit:
  image_data= convert_file_to_data(uploaded_file, 'image/jpeg')

  response= generate(input_prompt, image_data)
  # st.subheader("the response is:")
  
  response= str(response)
  print(response)
  start_idx=0
  end_idx=0
  for idx,char in enumerate(response):
    if char=='{':
      start_idx= idx
      break
  for idx,char in enumerate(response):
    if char=='}':
      end_idx= idx
  print(start_idx, end_idx)

  json_response= response[start_idx: end_idx+1]    
  print(json_response)

  data = json.loads(json_response)

  products = data['products']

  # Create a DataFrame from the list of products
  df_products = pd.DataFrame(products)

  st.header("Invoice Information")
  st.write(f"**Invoice no.:** {data['Invoice no.']}")
  st.write(f"**Location of the supermarket:** {data['location']}")
  st.write(f"**Supermarket Name:** {data['supermarket_name']}")
  st.write(f"**Date:** {data['Date']}")
  st.write(f"**Total Price:** {data['total_price']}")
  st.write(f"**Total number of products:** {data['total_products']}")
  st.write(f"**Mode of Payment:** {data['mode_of_payment']}")

  st.header("List of Products purchased")
  st.dataframe(df_products)