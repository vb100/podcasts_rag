"""
This Python file is developed with the purpose to retrieve the best chunks from the 
pre-generated vector database (ChromaDB)
"""

# Import modules and packages
import os
import logging
import configparser
import chromadb
from dotenv import load_dotenv
import chromadb.utils.embedding_functions as embedding_functions
from langchain_community.vectorstores import Chroma
from langchain.embeddings import SentenceTransformerEmbeddings


# Initialize logger
logging.basicConfig(
    filename="retrieval_pipeline.txt", encoding="utf-8", level=logging.INFO
)
template_name = "Chunks retrieval from vector database pipeline"
logger = logging.getLogger(template_name)

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

# System constants (from .env and config files)
DATABASE_NAME: str = None  # The most recent vector database found on 02 part
EMBEDDING_FUNCTION: str = conf["llm_parameters"]["EMBEDDING_FUNCTION"]
EMBEDDING_MODEL: str = conf["llm_parameters"]["EMBEDDING_MODEL"]


class RetrieveFromDB:
    """
    Retrieve the best corresponsing chunks from vector database based given user
    query. The extracted chunk(s) is being saved locally in JSON file format.
    """

    def __init__(
        self,
        embedding_function: str = EMBEDDING_FUNCTION,
        embedding_model: str = EMBEDDING_MODEL,
    ):
        self.embedding_function: str = embedding_function
        self.embedding_model: str = embedding_model

    def get_latest_vector_db_path(self, dir_path: str) -> None:
        """
        We need to take the latest generated vector database from <02> part and use
        this database to retrieve scores
        """
        pass

    def run_retrieval(self) -> dict:
        """
        Trigger the retrieval job and return the most corresponsive chunk(s)
        """

        pass


def main() -> None:
    """
    Run the retrieval pipeline in high level
    """
    job = RetrieveFromDB()
    job.run_retrieval()


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Retrieval from vector database")
    arg_parser.add_argument("--run", default=False, action="store_true")
    args = arg_parser.parse_args()

    if args.run:
        logger.info("Starting retrieval pipeline")
        # Run the pipeline
        main()
