import os
import sys
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from openai import OpenAI

load_dotenv(override=True)

QDRANT_URL = os.getenv("QDRANT_URL")
# QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL")
LLM_MODEL = os.getenv("LLM_MODEL")

required_vars = [QDRANT_URL, OPENAI_API_KEY, QDRANT_COLLECTION, EMBEDDING_MODEL, LLM_MODEL]

if not all(required_vars):
    print("Initialization Error: Missing variables in .env")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROMPT_FILE_PATH = os.path.join(BASE_DIR, "prompt", "prompt.md")

try:
    qdrant_client = QdrantClient(url=QDRANT_URL)
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
except Exception as e:
    print(f"Initialization Error: {e}")
    sys.exit(1)

def load_prompt_template(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Prompt file not found at {file_path}")
        sys.exit(1)
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def get_embedding(text):
    response = openai_client.embeddings.create(input=[text], model=EMBEDDING_MODEL)
    return response.data[0].embedding

def search_knowledge_base(query_vector, limit=3):
    response = qdrant_client.query_points(
        collection_name=QDRANT_COLLECTION,
        query=query_vector,
        limit=limit
    )
    return response.points

def generate_answer(query, contexts, prompt_template):
    context_text = "\n\n".join([f"Tai lieu {i+1}:\n{ctx}" for i, ctx in enumerate(contexts)])
    final_prompt = prompt_template.format(context=context_text, query=query)
    response = openai_client.chat.completions.create(
        model=LLM_MODEL,
        temperature=0.0,
        messages=[{"role": "user", "content": final_prompt}]
    )
    return response.choices[0].message.content

def main():
    print("VIETNAM LAW CHATBOT (LOCAL TERMINAL)")
    print("Type 'exit' or 'quit' to stop.")
    
    prompt_template = load_prompt_template(PROMPT_FILE_PATH)

    while True:
        user_query = input("\nUser: ").strip()
        if user_query.lower() in ['exit', 'quit']:
            break
        if not user_query:
            continue

        try:
            query_vector = get_embedding(user_query)
            search_hits = search_knowledge_base(query_vector, limit=5)
            
            if not search_hits:
                print("Assistant: Khong tim thay du lieu lien quan.")
                continue

            contexts = []
            source_details = []
            
            for i, hit in enumerate(search_hits):
                payload = hit.payload
                meta = payload.get("metadata", {})
                law_name = meta.get("law_name", "Unknown Law")
                article = meta.get("article", "Unknown Article")
                score = hit.score 
                
                contexts.append(payload.get("content", ""))
                source_details.append(f"[{i+1}] {law_name} - {article} (Cosine Similarity: {score:.4f})")
            
            final_answer = generate_answer(user_query, contexts, prompt_template)
            
            print("-" * 60)
            print(f"Assistant:\n{final_answer}")
            print("-" * 60)
            for source in source_details:
                print(source)
            print("-" * 60)
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()