# Import modules and packages
import pandas as pd
import requests
import json
import time
import logging
import re
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Import and use required excplicit functions and methods
from utils.utils import (
    prepare_page_for_scrapping,
    block_aggressively,
    save_to_json,
    error_msg_load_page,
    generate_scrapped_podcast_filename,
    parse_date,
)
from utils.preprocess_text import (
    preprocess_sentence,
    clean_paragprah_text,
    remove_timestamps,
    fix_urls_definitions
)


# Initialize logger
logging.basicConfig(
    filename="podcasts_scrapper_pipeline.txt", encoding="utf-8", level=logging.INFO
)
template_name = "Scrapping pipeline"
logger = logging.getLogger(template_name)


# System constants
MAX_RETRIES: int = 30  # maximum number of retries to load the review section
API_URL: str = 'https://www.superdatascience.com/fc1c3a35f8202c72aa2adac93ce2a764927351ad.js'  # Can be changed by API provider
MAIN_URL: str = 'https://www.superdatascience.com'
payload: dict = {
    'meteor_js_resource': 'true'
}
possible_text_tags: list = [
    '.transcript-container',
    '.block-animation'
]

class TextScrapper:
    """Run the scrapper by executing the following steps:
        - Collect all podcasts related URL links from the website.
        - Scrape all available text from these URL link.
        - Organize scrapped text storing actual text and its metadata.
        - Save all scrapped text locally."""
    
    def __init__(self, main_url=MAIN_URL, payload=payload, api_url=API_URL):
        self.main_url: str = main_url
        self.payload: str = payload
        self.api_url: str = api_url

    def get_response(self) -> list:
        """Get API response"""
        r = requests.get(self.api_url, json=self.payload)
        response = r.text.split('"redirects.js"')[-1].split('const a=')[-1].split(';t.autorun((()=>')[0]
        response = json.loads(response.replace('oldUrl', '"oldUrl"').replace('newUrl', '"newUrl"'))
        return response
    
    def scrape_podcasts_urls(self, response: str) -> list:
        """
        Analyze API response and collect podcast-related URLs with main metadata
        """
        if type(response) == list and len(response) > 0:
            l: list = []
            for i, this_link in enumerate(response):
                full_link: str = '/'.join([MAIN_URL, this_link['newUrl']])
                if '/podcast/' in full_link:
                    logger.info(f'Collecting URL: {i+1}: {this_link["newUrl"]}')
                    d: dict = {
                        'url': full_link,
                    }
                    l.append(dict(d))
            
            logger.info(f'Collection of podcast links is completed with {len(l)} records.')

        else:
            logger.error(f'Collected URLs are not in good format. Try again!')
            raise RuntimeError
        
        return l
    
    def handle_parapgraphs(self, all_text_sections: BeautifulSoup, tag: str) -> list:
        """
        Iterate through text paragrapsh whether they are based on <div> or <p> tags in the HTML code
        """
        l_text: list = []
        for this_paragraph in all_text_sections.find_all(tag):
            actual_text: str = clean_paragprah_text(paragraph_text=this_paragraph.text)
            if (len(actual_text) > 0) and (actual_text.upper() != 'Show all'.upper()):
                l_text.append(actual_text)

        return l_text
    
    def retry_on_load_page(self, url: str) -> bool:
        """
        Repeat load the page by given URL MAX_RETRIES times or till a page with full DOM is fully loaded using
        Playwright.
        """
        loaded: bool = True
        for _ in range(MAX_RETRIES):
            logger.info(f'::: attempt to load review section: {_ + 1} : {url}')
            try:
                loaded: bool = True
                self.page.goto(
                    f'{url}', wait_until="networkidle", timeout=25_000
                )
                self.page.set_default_timeout(60_000)  # 120 seconds
                break
            except:
                loaded: bool = False
                pass

        return loaded
    
    def load_dynamic_page(self, page_url: str) -> bool:
        """
        Load a dynamic page with Playwright and return error if fail
        """
        loaded: bool = self.retry_on_load_page(url=page_url)
        success: bool = True
        if not loaded:
            error_msg_load_page(url=page_url, max_retries=MAX_RETRIES)
            success: bool = not success

        return success
    
    def save_data_to_json(self, data: dict, filename: str) -> None:
        """
        Save scrapped data to external JSON file
        """
        save_to_json(data=data, filename=filename)

        return None
    
    def _full_podcast_text_cleaning_heuristic(self, podcast_text: str) -> str:
        """
        Steps to implement overall full podcast text cleaning and preparation
        """
        podcast_text: str = remove_timestamps(podcast_text=podcast_text)
        podcast_text: str = clean_paragprah_text(paragraph_text=podcast_text)
        podcast_text: str = fix_urls_definitions(podcast_text=podcast_text)

        return podcast_text
    
    def scrape_podcast_text(self, list_of_urls: list[dict]) -> list[dict]:
        """
        Receive collected podcast urls and scrape actual text from there
        """
        list_of_urls_ = list_of_urls

        with sync_playwright() as playwright:
            for i in range(0, len(list_of_urls)):
            #for i in range(201, len(list_of_urls)):
                this_record: dict = list_of_urls[i]
                logger.info(f'{i+1}, {this_record["url"]}')

                browser = playwright.chromium.launch(headless=True, slow_mo=1500)
                # <---- browsing logic: start
                self.page = browser.new_page()
                self.page.route("**/*", block_aggressively)
                self.load_dynamic_page(page_url=this_record['url'])

                if 'sds-' in this_record['url']:
                    page_title_: str = this_record['url'].split('sds-')[-1][4:]
                    podcast_number: str = re.search(r'[\w]{3}-[\d+]*', this_record['url']).group()
                    static_part, dynamic_part = podcast_number.split('-')
                    podcast_number: str = f'{static_part}-{str(int(dynamic_part)).zfill(4)}'
                elif 'podcast-' in this_record['url']:
                    page_title_: str = this_record['url'].split('podcast-')[-1]
                    podcast_number: str = f'cus-{str(i+1).zfill(4)}'
                elif '/podcast/' in this_record['url']:
                    page_title_: str = this_record['url'].split('/')[-1].strip()
                    podcast_number: str = f'cus-{str(i+1).zfill(4)}'
                
                page_title: str = page_title_.split(f'{podcast_number}: ')[-1]
                podcast_date: str = parse_date(
                    date_string=BeautifulSoup(
                        self.page.inner_html('.information'), 'html.parser'
                        ) \
                            .find_all('p')[-1].text) \
                            .strip()

                # Recognize the correct tag of HTML code where the actual podcast text is stored
                text_found: bool = False
                for this_text_tag in possible_text_tags:
                    if self.page.query_selector(this_text_tag) is not None:
                        html_text = self.page.inner_html(this_text_tag)
                        text_found: bool = True
                        break
                # browsing logic: end ---->
                browser.close()

                if text_found:
                    html_text_for_scrapping: BeautifulSoup = BeautifulSoup(html_text, 'html.parser')
                    l_text: list = []
                    if len(html_text_for_scrapping.find_all('p')) > 1:
                        l_text: list = self.handle_parapgraphs(all_text_sections=html_text_for_scrapping, tag='p')

                    elif len(html_text_for_scrapping.find_all('div')) > 0:
                        l_text: list = self.handle_parapgraphs(all_text_sections=html_text_for_scrapping, tag='div')
                    full_text: str = ' '.join(list(set(l_text)))
                    full_text: str = self._full_podcast_text_cleaning_heuristic(podcast_text=full_text)

                    
                    list_of_urls_[i]['full_text'] = full_text
                    list_of_urls_[i]['title'] = page_title
                    list_of_urls_[i]['date'] = podcast_date
                    list_of_urls_[i]['number'] = podcast_number
                    list_of_urls_[i]['n_words'] = len(full_text.split(' '))
                    list_of_urls_[i]['n_chars'] = len(full_text)
                    list_of_urls_[i]['n_sentences'] = len(full_text.split('. '))
                    list_of_urls_[i]['tokens_count'] = len(full_text) / 4  # 1 token = ~4 characters
                    # TODO: add timestamp of each podcast

                    # Generate filename and save the actual record (article text with metadata)
                    filename: str = generate_scrapped_podcast_filename(
                        title=list_of_urls_[i]["title"],
                        number=list_of_urls_[i]["number"],
                        date=podcast_date
                        )
                    self.save_data_to_json(
                        data=list_of_urls_[i],
                        filename=filename
                        )
                
                else:
                    logger.info(f'Text not found for {this_record["url"]}')

        return list_of_urls_

    def collect_podcast_urls_from_website(self):
        """
        Collect internal URLs addresses which stores podcast text inside (main function of the class)
        """
        response: list = self.get_response()
        podcast_urls: list[dict] = self.scrape_podcasts_urls(response=response)
        podcast_text: list[dict] = self.scrape_podcast_text(list_of_urls=podcast_urls)
        logger.info('Texts are colllected!')


def main():
    """Run scrapper pipeline"""
    job = TextScrapper()
    job.collect_podcast_urls_from_website()
    logger.info('Scrapper job is finished.')


if __name__ == '__main__':
    import argparse

    arg_parser = argparse.ArgumentParser(description='Podcast texts scrapper')
    arg_parser.add_argument('--run', default=False, action='store_true')
    args = arg_parser.parse_args()

    if args.run:
        logger.info('Starting podcast text scrapper')
        # Run the pipeline
        main()
