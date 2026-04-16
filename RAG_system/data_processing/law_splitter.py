import sys
import pdfplumber
import re
import os
import uuid
import json
import unicodedata

# ==============================================================================
# 1. INPUT / OUTPUT DIRECTORY CONFIGURATION
# ==============================================================================

# Directory containing the source PDF law files
INPUT_FOLDER = r"C:\Users\tabao\OneDrive\Desktop\Misa_first_week\RAG_system\data\input"
# Directory to store the processed JSON files
OUTPUT_FOLDER = r"C:\Users\tabao\OneDrive\Desktop\Misa_first_week\RAG_system\data\output"

# ==============================================================================
# 2. HELPER FUNCTIONS & PDF PROCESSING
# ==============================================================================

def clean_text(text):
    """
    Basic text cleaning: 
    Removes leading/trailing spaces and deletes empty lines.
    """
    if not text: return ""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines)

def generate_law_id(filename):
    """
    Generates a standardized ID for the law from the filename:
    - Removes the file extension.
    - Converts Vietnamese characters with accents to non-accented equivalents.
    - Replaces all non-alphanumeric characters with underscores (_).
    - Converts the entire string to uppercase.
    """
    name = os.path.splitext(filename)[0]
    name_no_accent = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    law_id = re.sub(r'[^a-zA-Z0-9]', '_', name_no_accent).upper()
    return re.sub(r'_+', '_', law_id).strip('_')

def extract_and_parse_pdf(pdf_path):
    """
    Extracts all text from the PDF and chunks it according to the hierarchy: 
    Chapter (Chương) -> Section (Mục) -> Article (Điều).
    """
    print(f"Reading and parsing file: {os.path.basename(pdf_path)}...")
    
    # Initialize regular expressions to capture the legal document structure
    re_chapter = re.compile(r'^Chương\s+([IVXLCDM\d]+)[\.\:\s]*(.*)', re.IGNORECASE)
    re_section = re.compile(r'^Mục\s+(\d+)[\.\:\s]*(.*)', re.IGNORECASE)
    re_article = re.compile(r'^Điều\s+(\d+[a-z]?)\.[\s:]*(.*)', re.IGNORECASE)

    records = []
    
    # Variables to store the current state of the hierarchy
    current_chapter = None
    current_section = None
    current_article_title = None
    current_article_content = []

    def save_current_article():
        """
        Internal function: Saves the content of the current 'Article' into the 
        records list before moving on to process the next 'Article'.
        """
        nonlocal current_article_title, current_article_content
        if current_article_title:
            content_str = "\n".join(current_article_content)
            record = {
                "chapter": current_chapter,
                "section": current_section,
                "article": current_article_title,
                "content": content_str
            }
            records.append(record)
            
            # Reset title and content to catch the new Article
            current_article_title = None
            current_article_content = []

    try:
        # Read the PDF file using pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
                txt = page.extract_text()
                if txt: full_text += txt + "\n"
            
        lines = clean_text(full_text).split('\n')
        
        # Iterate through each line to classify based on structure
        for line in lines:
            # Check if the line is a Chapter title
            match_chap = re_chapter.match(line)
            if match_chap:
                save_current_article() 
                current_chapter = line 
                current_section = None  # Reset Section when moving to a new Chapter
                continue

            # Check if the line is a Section title
            match_sec = re_section.match(line)
            if match_sec:
                save_current_article()
                current_section = line
                continue

            # Check if the line is an Article title
            match_art = re_article.match(line)
            if match_art:
                save_current_article()
                art_num = match_art.group(1)
                art_name = match_art.group(2).strip()
                full_title = f"Điều {art_num}" + (f". {art_name}" if art_name else "")
                
                current_article_title = full_title
                current_article_content = [line] 
                continue

            # If the line is not a title, append it to the content of the current Article
            if current_article_title:
                current_article_content.append(line)
        
        # Ensure the last Article is saved after exiting the loop
        save_current_article() 
        return records

    except Exception as e:
        print(f"Error processing PDF {os.path.basename(pdf_path)}: {e}")
        return []

# ==============================================================================
# 3. SAVE TO JSON FILE
# ==============================================================================

def save_to_json(data_records, output_path, law_id, law_name):
    """
    Appends metadata (UUID, law_id, order index) to the records 
    and exports them to JSON format.
    """
    if not data_records:
        print(f"No data to save for {law_name}.")
        return

    final_data = []
    order_index = 1

    # Create a complete schema for each document chunk
    for item in data_records:
        record = {
            "id": str(uuid.uuid4()),
            "law_id": law_id,
            "law_name": law_name,
            "chapter": item["chapter"],
            "section": item["section"],
            "article": item["article"],
            "content": item["content"],
            "order_index": order_index
        }
        final_data.append(record)
        order_index += 1

    try:
        with open(output_path, 'w', encoding='utf-8') as json_file:
            # ensure_ascii=False keeps the Vietnamese characters intact in the JSON
            json.dump(final_data, json_file, ensure_ascii=False, indent=4)
            
        print(f"Successfully saved {len(final_data)} articles to: {os.path.basename(output_path)}\n")

    except Exception as e:
        print(f"Error saving JSON file {os.path.basename(output_path)}: {e}\n")

# ==============================================================================
# MAIN FOLDER PROCESSING PIPELINE
# ==============================================================================

if __name__ == "__main__":
    # Check if the input folder exists
    if not os.path.exists(INPUT_FOLDER):
        print(f"Input directory not found: {INPUT_FOLDER}")
        sys.exit()

    # Ensure the output directory exists; create it if it doesn't
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Scan and get a list of all files with a .pdf extension
    pdf_files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        print(f"No PDF files found in directory {INPUT_FOLDER}")
    else:
        print(f"Found {len(pdf_files)} PDF file(s). Starting processing...\n")
        print("-" * 50)

        for filename in pdf_files:
            pdf_path = os.path.join(INPUT_FOLDER, filename)
            
            # Automatically extract the law name from the filename (remove .pdf extension)
            law_name = os.path.splitext(filename)[0]
            
            # Automatically generate law_id (e.g., "Luật Đất đai 2024" -> "LUAT_DAT_DAI_2024")
            law_id = generate_law_id(filename)
            
            # Parse the PDF text into a list of dictionaries
            parsed_data = extract_and_parse_pdf(pdf_path)
            
            # If parsing is successful, proceed to save the file
            if parsed_data:
                json_filename = f"{law_name}.json"
                output_path = os.path.join(OUTPUT_FOLDER, json_filename)
                
                save_to_json(parsed_data, output_path, law_id, law_name)
            
        print("COMPLETED PROCESSING THE ENTIRE FOLDER!")