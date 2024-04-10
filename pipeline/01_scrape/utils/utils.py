"""
This helper file support scrapper pipeline with the purpose to organize and implement
required actions.
"""

import time
import logging
import requests
from bs4 import BeautifulSoup

# Set-up a logger
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)


def prepare_page_for_scrapping(page_url: str) -> BeautifulSoup:
    """
    Build a BeautifulSoup object which will be suitable for scrapping data by a given webpage url address
    """
    r = requests.get(page_url)
    c = r.content
    return BeautifulSoup(c, 'html.parser')