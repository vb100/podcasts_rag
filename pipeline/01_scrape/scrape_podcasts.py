# Import modules and packages
import pandas as pd
import requests
import json
import time
import logging
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from utils.utils import (
    prepare_page_for_scrapping,
    block_aggressively,
    save_to_json
)


# Initialize logger
logging.basicConfig(
    filename="podcasts_scrapper_pipeline.txt", encoding="utf-8", level=logging.INFO
)
template_name = "Scrapping pipeline"
logger = logging.getLogger(template_name)


# System constants
API_URL: str = 'https://www.superdatascience.com/cc7f0e6a068fb7f727e59b28d5cdca71d73aa0c3.js'
MAIN_URL: str = 'https://www.superdatascience.com'
payload: dict = {
    'meteor_js_resource': 'true'
}

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
                    logger.info(i, this_link)
                    d: dict = {
                        'url': full_link,
                    }
                    l.append(dict(d))
            
            logger.info(f'Collection of podcast links is completed with {len(l)} records.')

        else:
            logger.error(f'Collected URLs are not in good format. Try again!')
            raise RuntimeError
        
        return l
    
    def scrape_podcast_text(self, list_of_urls: list[dict]) -> list[dict]:
        """
        Receive collected podcast urls and scrape actual text from there
        """
        list_of_urls_ = list_of_urls

        with sync_playwright() as playwright:
            #for i, this_record in enumerate(list_of_urls):
            for i, this_record in enumerate(list_of_urls[:10]):
                logger.info(i+1, this_record)

                browser = playwright.chromium.launch(headless=True, slow_mo=1500)
                # <---- browsing logic: start
                self.page = browser.new_page()
                self.page.route("**/*", block_aggressively)
                self.page.goto(this_record['url'], wait_until="networkidle", timeout=10_000)
                page_title_: str = self.page.title().split('|')[0].split(' - ')[0].strip()
                podcast_number: str = page_title_.split(': ')[0]
                page_title: str = page_title_.split(f'{podcast_number}: ')[-1]
                html_text = self.page.inner_html('.transcript-container')
                # browsing logic: end ---->
                browser.close()
                
                html_text_for_scrapping: BeautifulSoup = BeautifulSoup(html_text, 'html.parser')
                l_text: list = []
                for this_paragraph in html_text_for_scrapping.find_all('p'):  # Sometimes <p> is not exists
                    actual_text: str = this_paragraph.text.strip().replace('\t', ' ').replace('\n', '')
                    # TODO: the cleaning logic below can be re-formated into stand-alone function
                    if '(background music plays)' in actual_text:
                        actual_text: str = actual_text.replace('(background music plays)', '')
                    if (len(actual_text) > 0) and (actual_text.upper() != 'Show all'.upper()):
                        l_text.append(actual_text)

                full_text: str = '\n'.join(l_text)

                # Assignation to the original data-store
                list_of_urls_[i]['full_text'] = full_text
                list_of_urls_[i]['title'] = page_title
                list_of_urls_[i]['number'] = podcast_number


        return list_of_urls_
    
    def save_data_to_json(self, data: list[dict]) -> None:
        """
        Save scrapped data to external JSON file
        """
        save_to_json(data=data)

        return None

    def collect_podcast_urls_from_website(self):
        """
        Collect internal URLs addresses which stores podcast text inside (main function of the class)
        """
        response: list = self.get_response()
        podcast_urls: list[dict] = self.scrape_podcasts_urls(response=response)
        podcast_text: list[dict] = self.scrape_podcast_text(list_of_urls=podcast_urls)
        logger.info('Texts are colllected!')
        self.save_data_to_json(data=podcast_text)


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
