import os
import sys
import json
import re  # Mandatory for filtering garbage characters
from dotenv import load_dotenv

# Libraries for Qdrant and OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http import models
from openai import OpenAI

# Load environment variables
load_dotenv(override=True)

# ==============================================================================
# 1. SYSTEM AND DIRECTORY CONFIGURATION
# ==============================================================================

# Qdrant & OpenAI configuration
QDRANT_URL = os.getenv("QDRANT_URL") 
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY") 
OPENAI_API_KEY = os.getenv("OPENAI__API_KEY") 

# Set to the single new test collection
QDRANT_COLLECTION = "vietnam_law_test" 

OPENAI_EMBEDDING_MODEL = "text-embedding-3-large"
VECTOR_SIZE = 3072

# Path to the DIRECTORY containing multiple JSON files
INPUT_JSON_FOLDER = r"C:\Users\tabao\OneDrive\Desktop\Misa_first_week\RAG_system\data\output"

# ==============================================================================
# 2. TEXT PROCESSING AND EMBEDDING
# ==============================================================================

def clean_text_for_openai(text):
    """
    Clean hidden control characters from PDF to prevent OpenAI JSON 400 errors.
    """
    if not isinstance(text, str):
        return ""
    # Remove hidden control characters (except newline and tab)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
    # Remove carriage return and replace extra spaces
    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text.encode('utf-8', 'ignore').decode('utf-8')

def get_openai_embedding(text_list, client):
    """Call OpenAI API to create embeddings for a list of texts."""
    # Already cleaned in the outer step, no need to replace here
    response = client.embeddings.create(input=text_list, model=OPENAI_EMBEDDING_MODEL)
    return [data.embedding for data in response.data]

def process_and_insert_single_file(file_path, qdrant_client, openai_client):
    """Processing function for a single file"""
    # Read JSON file
    print(f"Reading file: {os.path.basename(file_path)}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data_records = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return

    if not data_records:
        print("Empty JSON file.")
        return

    print(f"Starting processing and uploading each Article (Total: {len(data_records)} Articles)...")
    print("-" * 50)

    # PROCESS EACH ITEM SEQUENTIALLY
    for item in data_records:
        article_name = item.get('article', 'Unknown Article')
        
        # 1. Tightly bind context to content (NEVER LOSE SEMANTICS)
        chapter_info = f" - {item['chapter']}" if item.get('chapter') else ""
        raw_text = (
            f"Văn bản pháp luật: {item.get('law_name', '')}{chapter_info}\n"
            f"Chi tiết: {article_name}\n"
            f"Nội dung quy định: {item.get('content', '')}"
        )

        # THIS STEP IS MANDATORY TO BYPASS THE 400 ERROR IN CERTAIN ARTICLES
        text_to_embed = clean_text_for_openai(raw_text)

        try:
            # 2. Call OpenAI Embedding for ONLY this 1 string
            vecs = get_openai_embedding([text_to_embed], openai_client)
            vector = vecs[0] # Get the first (and only) vector
            
            # 3. Package Payload and Point
            point = models.PointStruct(
                id=item["id"], 
                vector=vector,
                payload={
                    "content": item.get("content"),
                    "metadata": {
                        "law_id": item.get("law_id"),
                        "law_name": item.get("law_name"),
                        "chapter": item.get("chapter"),
                        "section": item.get("section"),
                        "article": item.get("article"),
                        "order_index": item.get("order_index")
                    }
                }
            )

            # 4. Upload immediately to Qdrant
            qdrant_client.upsert(collection_name=QDRANT_COLLECTION, points=[point])
            print(f"Processed & saved: {article_name}")

        except Exception as e:
            print(f"[Error at {article_name}]: {e}")
            # Use continue so if 1 Article fails API, it skips and proceeds to the next without crashing the program
            continue

    print("-" * 50)
    print(f"Completed file {os.path.basename(file_path)}!")

def process_entire_folder(folder_path):
    """Function to scan and process the entire directory"""
    # Initialize shared Clients for the whole process
    try:
        qdrant_client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        print("Successfully connected to Qdrant and OpenAI.")
    except Exception as e:
        print(f"[Initialization Error] Cannot connect to Qdrant/OpenAI: {e}")
        return

    # Check/Create Collection
    if not qdrant_client.collection_exists(QDRANT_COLLECTION):
        print(f"[Qdrant] Creating new collection '{QDRANT_COLLECTION}'...")
        qdrant_client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=models.VectorParams(size=VECTOR_SIZE, distance=models.Distance.COSINE)
        )

    # Get a list of JSON files
    json_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.json')]
    
    if not json_files:
        print(f"No JSON files found in directory: {folder_path}")
        return

    print(f"Found {len(json_files)} JSON files. Starting embedding process...")
    print("=" * 60)

    # Loop through each file and pass to the individual processing function
    for filename in json_files:
        file_path = os.path.join(folder_path, filename)
        process_and_insert_single_file(file_path, qdrant_client, openai_client)

# ==============================================================================
# MAIN PROGRAM
# ==============================================================================

if __name__ == "__main__":
    if not OPENAI_API_KEY:
        print("Missing OPENAI_API_KEY. Please check the .env file")
        sys.exit(1)

    if os.path.exists(INPUT_JSON_FOLDER):
        print("=" * 60)
        print("STARTING EMBEDDING PROCESS FOR THE ENTIRE FOLDER...")
        print("=" * 60)
        process_entire_folder(INPUT_JSON_FOLDER)
        print("=" * 60)
        print("EMBEDDING PROCESS FOR ALL FILES HAS BEEN COMPLETED!")
    else:
        print(f"Directory not found: {INPUT_JSON_FOLDER}")