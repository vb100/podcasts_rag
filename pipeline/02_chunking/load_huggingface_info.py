"""
This Python file is dedicated to load credentials (API Key and/or Token) and 
model name to connect to HuggingFace API and load specifiend embedding model
from there
"""

# Import modules and packages
import os
from dotenv import load_dotenv


def load_hugging_face_creds():
    """
    Load Hugging Face credentials from .env local file
    """

    load_dotenv()
    api_token: str = os.environ.get("HF_API_TOKEN")
    embedding_function: str = os.environ.get("EMBEDDING_FUNCTION")
    embedding_model: str = os.environ.get("EMBEDDING_MODEL")

    return {
        "api_token": api_token,
        "embedding_function": embedding_function,
        "embedding_model": embedding_model,
    }
