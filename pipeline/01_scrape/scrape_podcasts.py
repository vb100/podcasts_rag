# Import modules and packages
import pandas as pd
import requests
import json
import time
import logging


# Set-up a logger
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)


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
                podcast_number: str = this_link['oldUrl'].split('/')[-1]
                if '/podcast/' in full_link:
                    print(i, this_link)
                    d: dict = {
                        'url': full_link,
                        'number': podcast_number
                    }
                    l.append(dict(d))
            
            logger.info(f'Collection of podcast links is completed with {len(l)} records.')

        else:
            logger.error(f'Collected URLs are not in good format. Try again!')
            raise RuntimeError
        
        return l



    def collect_podcast_urls_from_website(self):
        """
        Collect internal URLs addresses which stores podcast text inside
        """
        
        response: list = self.get_response()
        podcast_urls: list[dict] = self.scrape_podcasts_urls(response=response)

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
