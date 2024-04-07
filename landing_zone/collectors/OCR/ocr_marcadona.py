import cv2
import pytesseract
import re
import json
from pathlib import Path
import configparser


def find_store_name(text):
    lines = text.split('\n')
    for line in lines:
        if re.search(r'^[ ,]*[A-Z]{2,}[ ,.]*[A-Z]*', line):
            return re.sub(r'[ ,.]*$', '', line).strip()
    return None

def find_store_address(text):
    lines = text.split('\n')
    street_indicators = ['CALLE', 'C.', 'CARRER', 'CTRA.', 'AV.', 'AVE', 'PLAZA', 'PZA.', 'VIA', 'CAMINO', 'CLL', 'CTRA', 'CIRA']
    pattern = re.compile(r'(?:' + '|'.join(street_indicators) + r')\.?\s+.*?\d+', re.IGNORECASE)
    for line in lines:
        if pattern.search(line):
            return line.strip()
    return None

def process_image(image_path,tesseract_config, tesseract_lang):
    image = cv2.imread(str(image_path))
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    text = pytesseract.image_to_string(thresh, lang=tesseract_lang, config=tesseract_config).replace('\\', ' ').replace('+', '').replace(',', '.')
    store_name = find_store_name(text)
    store_address = find_store_address(text)
    item_pattern = re.compile(r'(\d+)?\s*([A-ZÀ-ÿ\s%]+[A-ZÀ-ÿ]+)\s+(\d+\.\d{2})\s*(\d+\.\d{2})?')
    matches = item_pattern.findall(text)
    items_details = [{'quantity': int(match[0] or '1'), 'description': match[1].strip(), 'unit_price': float(match[2]), 'total_price': float(match[3] or match[2])} for match in matches]
    return {'store_name': store_name, 'store_address': store_address, 'items': items_details}

def process_multiple_images(image_paths,tesseract_config, tesseract_lang):
    all_details = []
    for image_path in image_paths:
        image_details = process_image(image_path,tesseract_config, tesseract_lang)
        image_details['image_path'] = str(image_path)  # Include image path in details
        all_details.append(image_details)
    return all_details


def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config



def main(config_path):
    config = load_config(config_path)

    # Accessing configuration values
    image_directory = config['OCR']['ImageDirectory']
    output_json = config['OCR']['OutputJSON']
    image_file_types = config['OCR']['ImageFileTypes'].split(', ')
    tesseract_config = config['OCR']['TesseractConfig']
    tesseract_lang = config['OCR']['TesseractLang']

    image_folder = Path(image_directory)
    image_paths = []
    for file_type in image_file_types:
        image_paths.extend(image_folder.glob(file_type))
    all_extracted_details = process_multiple_images(image_paths, tesseract_config, tesseract_lang)

    # Serialize to JSON and save to file
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(all_extracted_details, f, indent=2, ensure_ascii=False)

    print(f"Details extracted and saved to {output_json}.")

if __name__ == "__main__":
    main("../../config.ini")
