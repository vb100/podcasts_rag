"""
This helper file support text cleaning and pre-processing before saving scrapping output as JSON file
"""

import re

def preprocess_sentence(sentence: str) -> str:
    """
    Pre-process scrapped and split sentence to be readable and usable for chunking.
    """
    # Handle cases: "e.9"
    d: dict = {}
    for this_token in sentence.split(' '):
        if re.search(r'[^1-9][.]\d+', this_token):
            temp_seq: list = this_token.split('.')
            fixed_seq: str = f'{temp_seq[0]}. {temp_seq[1]}'
            d[this_token] = fixed_seq
    for key, value in d.items():
        sentence: str = sentence.replace(key, value)

    # Handle case: 9Self-service
    d: dict = {}
    pattern_digit: str = r'\d+'
    pattern_word: str = r'[a-zA-Z]'

    for this_token in sentence.split(' '):
        if re.search(r'\d+[a-zA-Z]', this_token):
            temp_seq: str = re.search(r'\d+[a-zA-Z]', this_token).group()
            digit: str = re.search(pattern_digit, temp_seq).group()
            word: str = re.search(pattern_word, temp_seq).group()
            fixed_seq: str = f'{digit} {word}'
            d[temp_seq] = fixed_seq
    for key, value in d.items():
        sentence: str = sentence.replace(key, value)

    # Handle case: benefitsQuitting
    d: dict = {}
    pattern_cap: str = r'[A-Z]'
    pattern_non_cap: str = r'[a-z]'

    for this_token in sentence.split(' '):
        if re.search(r'[a-z][A-Z]', this_token):
            temp_seq: str = re.search(r'[a-z][A-Z]', this_token).group()
            non_cap_char: str = re.search(pattern_non_cap, temp_seq).group()
            cap_char: str = re.search(pattern_cap, temp_seq).group()

            fixed_seq: str = f'{non_cap_char} {cap_char}'
            d[temp_seq] = fixed_seq

    for key, value in d.items():
        sentence: str = sentence.replace(key, value)

    # Handle case: prohibited.Underage
    d: dict = {}

    for this_token in sentence.split(' '):
        if re.search(r'[a-z][.][A-Z]', this_token):
            temp_seq: str = re.search(r'[a-z][.][A-Z]', this_token).group()
            non_cap_char: str = re.search(pattern_non_cap, temp_seq).group()
            cap_char: str = re.search(pattern_cap, temp_seq).group()
            fixed_seq: str = f'{non_cap_char}. {cap_char}'
            d[temp_seq] = fixed_seq
    for key, value in d.items():
        sentence: str = sentence.replace(key, value)
            
    # Handle case: while?Artem
    d: dict = {}
    for this_token in sentence.split(' '):
        if re.search(r'[a-zA-Z]+[?][a-zA-Z]+', this_token):
            temp_seq: str = re.search(r'[a-zA-Z]+[?][a-zA-Z]+', this_token).group()
            word_1, word_2 = temp_seq.split('?')
            fixed_seq: str = f'{word_1}? {word_2}'
            d[temp_seq] = fixed_seq
            
    for key, value in d.items():
        sentence: str = sentence.replace(key, value)
    
    # Handle case: 2019.3
    d: dict = {}

    for this_token in sentence.split(' '):
        if re.search(r'[\d+]{4}[.][\d+]', this_token):
            temp_seq: str = re.search(r'[\d+]{4}[.][\d+]', this_token).group()
            digit_1, digit_2 = temp_seq.split('.')
            fixed_seq: str = f'{digit_1}. {digit_2}'
            d[temp_seq] = fixed_seq

    for key, value in d.items():
        sentence: str = sentence.replace(key, value)

    return sentence