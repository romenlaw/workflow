import os
import requests
import openai

# CBA GenAI Studio API configuration
GENAI_API_URL = os.getenv('GENAI_API_URL')
GENAI_API_KEY = os.getenv('GENAI_API_KEY')
CHAT_MODEL='gpt-4.1_v2025-04-14_GLOBAL'
EMBED_MODEL='text-embedding-3-large_v1'
client=openai.OpenAI(api_key=GENAI_API_KEY, base_url=GENAI_API_URL, timeout=300)

def get_available_models():
    """Get list of available models from the API."""
    headers = {
        'Authorization': f'Bearer {GENAI_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(
        f'{GENAI_API_URL}/v1/models',
        headers=headers
    )
    
    if response.status_code == 200:
        return response.json()['data']
    else:
        raise Exception(f"Error getting models: {response.text}")
    
def get_available_emb_models():
    """Get list of available embedding models from the API"""
    all_models = get_available_models()
    emb_models = [datum['id'] for datum in all_models if 'emb' in datum['id']]
    return emb_models

def get_basename_without_extension(file_path):
    # Extract the basename (filename with extension)
    base_name = os.path.basename(file_path)
    # Split the base name and the extension
    name, _ = os.path.splitext(base_name)
    return name

def unicode_escape_if_outside_utf8(s):
    return ''.join(f'\\u{ord(ch):04x}' if ord(ch) > 127 else ch for ch in s)

