"""
This Python file is developed with the purpose to chunk scrapped podcast text into smaller chunks and
save them into new vector database (ChromaDB)
"""

# Import packages and modules
import os
import logging
import configparser
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
conf_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        os.environ.get("CONFIG_PATH"),
    )
)

conf = configparser.ConfigParser()
conf.read(os.path.join(conf_dir, "config.conf"))

# Initialize logger
logging.basicConfig(
    filename="chunking_saving_pipeline.txt", encoding="utf-8", level=logging.INFO
)
template_name = "Podcast chunking and saving pipeline"
logger = logging.getLogger(template_name)

# System constants (from .env and config files)
DATABASE_NAME: str = f"db_{get_current_date_and_time()}"
CHUNKS_OVERLAP_RATIO: int = int(conf["llm_parameters"]["CHUNKS_OVERLAP_RATIO"])
CHUNK_SIZE: int = int(conf["llm_parameters"]["CHUNK_SIZE"])  # Tokens
L: int = int(conf["llm_parameters"]["LENGHT_OF_SENTENCE"])  # Length of sentence allowed
EMBEDDING_FUNCTION: str = conf["llm_parameters"]["EMBEDDING_FUNCTION"]
EMBEDDING_MODEL: str = conf["llm_parameters"]["EMBEDDING_MODEL"]


class ChunkingAndSaving:
    """
    Chunk the given bunch of scrapped text collections, initialize a new ChromaDB and save all
    chunks there
    """

    def __init__(
        self,
        db_name: str = DATABASE_NAME,
        chunks_overlap: int = CHUNKS_OVERLAP_RATIO,
        chunk_size: int = CHUNK_SIZE,
        embedding_function: str = EMBEDDING_FUNCTION,
        embedding_model: str = EMBEDDING_MODEL,
    ):
        self.vectordb_name: str = db_name
        self.chunks_overlap: int = chunks_overlap
        self.chunk_size: int = chunk_size
        self.embedding_function: str = embedding_function
        self.embedding_model: str = embedding_model

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
            model_name=self.embedding_function,
        )
        # Use these credentials on demand if needed

        embeddings = SentenceTransformerEmbeddings(model_name=self.embedding_model)

        return embeddings


def main():
    """
    Run chunking and saving to vectordb pipeline
    """

    job = ChunkingAndSaving()

    # Load scrapped data
    text_with_data: list[dict] = job.load_all_jsons(
        path="../01_scrape/output", extension=".json"
    )

    # Initialize VectorDB
    vector_db = Chroma(
        persist_directory="./" + DATABASE_NAME,
        embedding_function=job.connect_to_hugging_face(),
    )

    for this_collection in tqdm(text_with_data):
        full_text: str = this_collection["full_text"]

        # 1. Split text to sentences
        sentences: list = list(job.nlp(full_text).sents)

        full_text_l: list = []
        for this_sentence in sentences:
            sentence: str = preprocess_sentence(sentence=str(this_sentence))
            if valid_sentence(sentence=sentence, allowed_sentence_lenght=L):
                full_text_l.append(sentence)
            full_text_joined: str = " ".join(full_text_l)

        collection_name: str = this_collection["title"][0:61]
        if collection_name.endswith("-"):
            collection_name = collection_name[: len(collection_name) - 1]
        collection_name: str = collection_name.replace("-", "_")

        chunks: list = split_text(
            text=full_text_joined,
            chunk_overlap=job.chunks_overlap,
            chunk_size=job.chunk_size,
        )
        metadata: list = [{"source": this_collection["title"]} for name in chunks]

        logger.info(
            f"Initializing VectorDB and pushing the document: {collection_name}"
        )

        vector_db.add_texts(
            texts=chunks, metadatas=metadata, collection_name=collection_name
        )

    vector_db.persist()

    logger.info("The full pipeline is completed.")


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Podcast texts chunking")
    arg_parser.add_argument("--run", default=False, action="store_true")
    args = arg_parser.parse_args()

    if args.run:
        logger.info("Starting chunking and saving pipeline")
        # Run the pipeline
        main()
