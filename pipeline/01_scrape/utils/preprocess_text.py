"""
This helper file support text cleaning and pre-processing before saving scrapping output as JSON file
"""

import re

def preprocess_sentence(sentence: str) -> str:
    """
    Pre-process scrapped and split sentence to be readable and usable for NER.
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
            
    # Handle case: needed."Kirill:
    d: dict = {}
        
    for this_token in sentence.split(' '):
        if re.search(r'[a-zA-Z]+[.][\"][a-zA-Z]+', this_token):
            temp_seq: str = re.search(r'[a-zA-Z]+[.][\"][a-zA-Z]+', this_token).group()
            word_1, word_2 = temp_seq.split('.')
            fixed_seq: str = f'{word_1}. {word_2}'
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

def clean_paragprah_text(paragraph_text: str) -> str:
    """
    Clean paragprah text by removing phrases and words which do not make any sense to the LLM
    """

    paragraph_text: str = paragraph_text.strip()
    phrases_to_remove: dict = {
        '\t': ' ',
        '\n': ' ',
        '--':'-',
        "I'll ": 'I will ',
        "I’ll": "I will",
        "you're ": "you are ",
        "they’re": "they are",
        "They’re": "They are",
        "we'll ": "we will ",
        "We'll": "We will",
        "we'll": "we will",
        "don't": "do not",
        "Don't": "Do not",
        "we've": "we have",
        "We've": "We have",
        "they’d": "they would",
        "They’d": "They would",
        " ...": ".",
        "(Laughs)": "",
        "there’ll": "there will",
        "There’ll": "There will",
        "Can't": "Can not",
        "can't": "can not",
        "He’s": "He is",
        "You’d": "You would",
        "you’d": "you would",
        "there’re": "there are",
        "who’s": "who is",
        "they’ve": "they have",
        "They’ve": "They have",
        "You'll ": "You will ",
        "you'll": "you will",
        "won’t": "will not",
        "that’ll": "that will",
        "wasn’t": "wasn not",
        "didn't": "Did not ",
        "Here’s": "Here is",
        "we’d": "we would",
        "We’d": "We would",
        "you've": "you have",
        "what's": "what is",
        "We’re": "We are",
        "don’t": "do not",
        "Don’t": "Do not",
        "you’d": "you would",
        "didn’t": "did not",
        "Can’t": "Can not",
        "What’s": "What is",
        "wouldn’t": "would not",
        "we’ve": "we have",
        "We’ve": "We have",
        "There’ll": "There will",
        "can’t": "can not",
        "we’re": "we are",
        "I’d": "I would",
        "shouldn’t": "should not",
        "there’s": "there is",
        "you’ll": "you will",
        "You’ll": "You will",
        "There’s": "There is",
        "else’s": "else is",
        "I'm ": "I am ",
        "I’m": "I am",
        "couldn’t": "could not",
        "They’ll": "They will",
        "we're": "we are",
        "they’ll": "they will",
        "We’ll": "We will",
        "we’ll": "we will",
        "he’s": "he is",
        "doesn’t": "does not",
        "I've": "I have",
        "you’ve": "you have",
        "You’ve": "You have",
        "It's": "It is",
        "it's ": "it is ",
        "aren’t": "are not",
        "Aren’t": "Are not",
        "isn’t": "is not",
        "what’s": "what is",
        "haven’t": "have not",
        "Haven’t": "Have not",
        "it’s ": "it is ",
        "It’s": "It is",
        "I’ve": "I have",
        "he’ll": "he will",
        "He’ll": "He will",
        "that's ": 'that is ',
        "wasn't": "was not",
        "That’s": "That is",
        "that’s": "that is",
        "they'll": "they will",
        "there‘s": "there is",
        "That's ": "That is ",
        "he's": "he is",
        "He's": "He is",
        "there's ": "there is ",
        "they're": "they are",
        "They're": "They are",
        "hasn’t": "has not",
        "Hasn’t": "Has not",
        "doesn't ": "does not",
        "you’re": "you are",
        "You’re": "You are",
        "there's": "there is",
        "There's": "There is",
        "--": "-",
        'Podcast Transcript': '',
        '(background music plays)': '',
        " ] ": "] ",
        " . ": ". ",
        "...": ".",
        "?." : "?",
        "!." : "!"
    }

    for this_key in phrases_to_remove.keys():
        paragraph_text: str = paragraph_text.replace(this_key, phrases_to_remove.get(this_key))

    paragraph_text: str = preprocess_sentence(sentence=paragraph_text.replace('  ', ' '))
    paragraph_text: str = " ".join(paragraph_text.split())

    return paragraph_text

def remove_timestamps(podcast_text: str) -> str:
    """
    Remove found timestamps [HH:MM:SS] from the given podcast text and return
    cleaned version
    """
    timestamp_patterns: dict = {
        r'[[]\d[:]\d+[:]\d+[]]': '',  # [HH:MM:SS]
        r'\s[[][\w+]*[ ]\b\d+[:]\d+[:]\d+[]]': '',  # [inaudible HH:MM:SS]
        r'\s[(]\d+:\d+[)]': '. ',  # (13:44),
        r'\s[[]\d+:\d+[]][A-Z]': '. ' # [33:28]Parameters
    }

    for this_timestamp_pattern in timestamp_patterns.keys():
        timestamps = re.findall(this_timestamp_pattern, podcast_text)
        if len(timestamps) > 0:
            for this_timestamp in timestamps:
                podcast_text: str = podcast_text.replace(this_timestamp, timestamp_patterns.get(this_timestamp_pattern))

    return podcast_text