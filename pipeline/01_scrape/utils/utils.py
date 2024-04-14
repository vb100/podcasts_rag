"""
This helper file support scrapper pipeline with the purpose to organize and implement
required actions.
"""

import time
import logging
import requests
import os
from bs4 import BeautifulSoup
import json

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


def block_aggressively(route, request, excluded_resource_types=["image", "images"]):
    """
    Blocks all resources except the ones in the excluded_resource_types list with playwright
    By default just blocks images
    """
    # excluded_resource_types=["stylesheet", "script", "image", "images", "font"]
    if request.resource_type in excluded_resource_types:
        route.abort()
    else:
        route.continue_()


def save_to_json(data: dict, filename: str) -> None:
    """
    Save a given list with data to JSON file
    """
    data_folder: str = 'output'
    with open(os.path.join(os.getcwd(), data_folder, filename), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info('JSON output is saved successfully.')


def error_msg_load_page(url: str, max_retries: str) -> None:
    """
    Error message in case of failed load review section
    """
    logger.error(f'The reviews section was not loaded in {max_retries} times!')
    logger.error(f'| Page URL: {url}')
    logger.error('-'*20)


def generate_scrapped_podcast_filename(title: str, number: str) -> str:
    """
    Clean a given raw filename string and return generated version of that
    """
    filename: str = f'{number.replace(" ", "_").lower()}_{title.lower().split(":")[-1].replace(" ", "_")}.json'

    rules_to_clean: dict = {
        " ": "_",
        '/': '_',
        ',': '',
        '”': '',
        '%': '',
        '!': '',
        '"': '',
        '?': '',
        '__': '_',
        'www.': '',
    }

    for this_key in rules_to_clean.keys():
        filename: str = filename.replace(this_key, rules_to_clean.get(this_key))

    return filename