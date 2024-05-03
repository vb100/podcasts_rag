"""
This Python file is developed with the purpose to chunk scrapped podcast text into smaller chunks and
save them into new vector database (ChromaDB)
"""

# Import packages and modules
import logging
import json
from tqdm.auto import tqdm
from dotenv import load_dotenv
from spacy.lang.en import English
import chromadb
from langchain_community.vectorstores import Chroma
from langchain.embeddings import SentenceTransformerEmbeddings
import chromadb.utils.embedding_functions as embedding_functions
from load_huggingface_info import load_hugging_face_creds
from utils import (
    get_file_list,
    read_single_json,
    get_current_date_and_time,
    preprocess_sentence,
    valid_sentence,
    split_text,
)

# Load config and environment
load_dotenv()

# Initialize logger
logging.basicConfig(
    filename="chunking_saving_pipeline.txt", encoding="utf-8", level=logging.INFO
)
template_name = "Podcast chunking and saving pipeline"
logger = logging.getLogger(template_name)

# System constants
DATABASE_NAME: str = f"db_{get_current_date_and_time()}"
CHUNKS_OVERLAP_RATIO: int = (
    10  # How many common characters allowed in between two sequential chunks
)
MIN_TOKEN_LENGHT: int = 35  # Minimum allowed number of tokens per sentence
L: int = 35  # Length of sentence allowed
NUM_SENTENCES_CHUNK_SIZE: int = 12  # split size to turn groups of sentences into chunks


class ChunkingAndSaving:
    """
    Chunk the given bunch of scrapped text collections, initialize a new ChromaDB and save all
    chunks there
    """

    def __init__(
        self,
        db_name: str = DATABASE_NAME,
        chunks_overlap: int = CHUNKS_OVERLAP_RATIO,
        min_token_lenght: int = MIN_TOKEN_LENGHT,
        num_sentence_chunk_size: int = NUM_SENTENCES_CHUNK_SIZE,
    ):
        self.vectordb_name: str = db_name
        self.chunks_overlap: str = chunks_overlap
        self.min_token_lenght: int = min_token_lenght
        self.num_sentence_chunk_size: int = num_sentence_chunk_size

        # Add sentencizer pipeline
        self.nlp = English()
        self.nlp.add_pipe("sentencizer")

    def load_all_jsons(self, path: str, extension: str) -> list[dict]:
        """
        Find and load all scrapped JSON files with podcast texts
        """

        l_urls: list = []
        json_files: list[dict] = get_file_list(root_dir=path, E=extension)
        if len(json_files) > 0:
            l_jsons: list[dict] = []
            for this_file in json_files:
                if this_file.endswith(extension):
                    data_file: dict = read_single_json(path=this_file)
                    url: str = data_file["url"]
                    if url not in l_urls:
                        l_urls.append(url)
                        l_jsons.append(data_file)

            return l_jsons
        else:
            return None

    def connect_to_hugging_face(self):
        """
        Load ensembling model from HuggingFace
        """

        loaded_info: dict = load_hugging_face_creds()

        huggingface_ef = embedding_functions.HuggingFaceEmbeddingFunction(
            api_key=loaded_info.get("api_token"),
            model_name=loaded_info.get("embedding_function"),
        )
        # Use these credentials on demand if needed

        embeddings = SentenceTransformerEmbeddings(
            model_name=loaded_info.get("embedding_model")
        )

        return embeddings

    def split_list(self, input_list: list[str], slice_size: int) -> list[list[str]]:
        """
        Split list of sentences into chunks by slice size
        """
        return [
            input_list[i : i + slice_size]
            for i in range(0, len(input_list), slice_size)
        ]


def main():
    """
    Run chunking and saving to vectordb pipeline
    """

    job = ChunkingAndSaving()

    # Load scrapped data
    text_with_data: list[dict] = job.load_all_jsons(
        path="../01_scrape/output", extension=".json"
    )

    # Load ensembling function from Hugging Face
    huggingface_ef = job.connect_to_hugging_face()

    # Initialize VectorDB
    vector_db = Chroma(
        persist_directory="./" + DATABASE_NAME, embedding_function=huggingface_ef
    )

    for this_collection in tqdm(text_with_data[:5]):
        full_text: str = this_collection["full_text"]

        # 1. Split text to sentences
        sentences: list = list(job.nlp(full_text).sents)

        full_text_l: list = []
        for this_sentence in sentences:
            sentence: str = preprocess_sentence(sentence=str(this_sentence))
            if valid_sentence(sentence=sentence):
                full_text_l.append(sentence)
            full_text_joined: str = " ".join(full_text_l)

        collection_name: str = this_collection["title"][0:61]
        if collection_name.endswith("-"):
            collection_name = collection_name[: len(collection_name) - 1]
        collection_name: str = collection_name.replace("-", "_")

        chunks: list = split_text(text=full_text_joined)
        metadata: list = [{"source": this_collection["title"]} for name in chunks]

        logger.info(
            f"[INFO] Initializing VectorDB and pushing the document: {collection_name}"
        )

        vector_db.add_texts(
            texts=chunks, metadatas=metadata, collection_name=collection_name
        )

    vector_db.persist()

    logger.info("The full pipeline is completed.")


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Podcast texts scrapper")
    arg_parser.add_argument("--run", default=False, action="store_true")
    args = arg_parser.parse_args()

    if args.run:
        logger.info("Starting chunking and saving pipeline")
        # Run the pipeline
        main()
