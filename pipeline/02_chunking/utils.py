"""
Helper functions to support text chunking and saving to vectordb
"""

# Import modules and packages
from datetime import datetime
import os
import re
import json


def get_file_list(root_dir, E):
    """
    Get all files with given extenstion in a root folder
    """
    file_list = []

    for root, directories, filenames in os.walk(root_dir):
        for filename in filenames:
            if any(ext in filename for ext in E) and not filename.endswith(".DS_Store"):
                file_list.append(os.path.join(root, filename))

    return file_list


def read_single_json(path: str) -> dict:
    """
    Read a single JSON file from given path
    """
    with open(path, encoding="utf-8") as fh:
        json_data = json.load(fh)

    return json_data


def preprocess_chunk(chunk: str) -> str:
    """
    Execute required steps to pre-process a given chunk to be better embedded into vectorDB
    """

    joined_sentence_chunk = re.sub(r"\.([A-Z])", r". \1", chunk)
    joined_sentence_chunk = re.sub(r"\!([A-Z])", r"! \1", joined_sentence_chunk)
    joined_sentence_chunk = re.sub(r"\?([A-Z])", r"? \1", joined_sentence_chunk)

    joined_sentence_chunk = joined_sentence_chunk.replace(".:", ". ")
    joined_sentence_chunk = joined_sentence_chunk.replace("..", ".")
    joined_sentence_chunk = joined_sentence_chunk.replace("Py Torch", "PyTorch")

    return joined_sentence_chunk


def preprocess_sentence(sentence: str) -> str:
    """
    Pre-process the given sentence before pushing it to vector databse
    """
    replacements: dict = {
        ".:": ". ",
        "Py Torch": "PyTorch",
    }

    for this_key in replacements.keys():
        sentence: str = sentence.replace(this_key, replacements.get(this_key))

    return sentence


def valid_sentence(sentence: str, allowed_sentence_lenght: int) -> bool:
    """
    Check if the given sentence is valid to push it to vector database
    """

    CONDS = [
        "Data Science Coach and Lifestyle Entrepreneur".lower() not in sentence.lower(),
        "Welcome to the Super Data Science Podcast".lower() not in sentence.lower(),
        "look forward to seeing you".lower() not in sentence.lower(),
        "happy analyzing".lower() not in sentence.lower(),
        "This is Five-Minute Friday on".lower() not in sentence.lower(),
        "I was really excited".lower() not in sentence.lower(),
        "see you back here next time".lower() not in sentence.lower(),
        len(sentence) > allowed_sentence_lenght,
    ]

    if all(CONDS):
        return True
    else:
        return False


def split_text(text: str, chunk_overlap: int, chunk_size: int) -> list:
    """
    Splitting given text into smaller chunks
    """
    tokens: list = text.split()
    splits: list = []
    temp_list: list = []
    cursor: int = 0

    while cursor < len(tokens):
        temp_list.append(tokens[cursor])

        if len(temp_list) == chunk_size:
            cursor -= chunk_overlap
            splits.append(" ".join(temp_list))
            temp_list = []

        cursor += 1

    splits.append(" ".join(temp_list))

    return splits


def get_current_date_and_time() -> str:
    """
    Get current timestamp (date and time) in single string
    """
    now = datetime.now()

    return now.strftime("%Y%m%d_%H%M%S")
