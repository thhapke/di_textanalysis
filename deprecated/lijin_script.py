import os
import re
from tqdm import tqdm
import sklearn
import nltk

nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer
import gensim
import pickle
import itertools
import numpy as np
import pandas as pd
import string
import random
from matplotlib import pyplot as plt
import seaborn as sns
from hdfs import InsecureClient
from datetime import date

folder_path = "/shared/EU_crawled_texts/"


class WordCleaner:
    def __init__(self, ):
        # chunk_size = api.config.chunk_size
        self.article_count = 0
        self.diff_count = -1
        self.sent = False

        # get the stopwords list from NLTK
        self.stop_words = stopwords.words('german')
        additional_stopwords = ['spiegel', 'sagte', 'sagten', 'wurde', 'wurden', 'sei', 'sein', 'seien']
        additional_stopwords.extend(list(string.ascii_lowercase))
        additional_stopwords.extend([str(ii) for ii in range(0, 100)])
        self.stop_words.extend(additional_stopwords)
        # set the tokenizer object of words or digits
        self.tokenizer = RegexpTokenizer(r'\w+|\d+')
        self.articles_string = []
        self.articles_list = []

    def get_message(self, message):
        self.article = str(message.body, encoding='utf-8')
        # aritlce_name = str(message.storage.filename)
        self.article_count += 1
        log = f'article-{self.article_count} received'
        api.send('processbar', log)

    def clean(self):

        article = self.article
        # set all words to lower case
        article = article.lower()
        # special words like u.s. should be taken care of
        # article = article.replace('u.s.', 'united states')
        # article = article.replace('us', 'united states')
        article = self.tokenizer.tokenize(article)
        # unifying singular and plural words
        # article = [plural_stemmer(word) for word in article]
        # removing stop words with the function from gensim
        # article = gensim.parsing.preprocessing.remove_stopwords(article)
        article = [word for word in article if word not in self.stop_words]
        # remove words that only occur once in an article
        article = [word for word in article if article.count(word) > 1]
        # remove all empty strings
        article_list = list(filter(None, article))
        # convert list into srting
        article_string = ' '.join(article_list)
        # strip whitespace
        article_string = article_string.strip()
        # remove single letters
        article_string = re.sub(r'\b[a-z]\b', '', article_string)

        self.articles_list.append(article_list)
        self.articles_string.append(article_string)
        log = f"article-{self.article_count} cleaned"
        api.send('processbar', log)

    def check_input(self):
        log = f'input checked, difference count {self.diff_count}, article_count {self.article_count}'
        api.send('processbar', log)
        if self.article_count > 0:
            if self.diff_count < self.article_count:
                self.diff_count = self.article_count
                self.no_input = False
            else:
                self.no_input = True
        else:
            self.no_input = False

    def send_message(self):
        if (self.no_input and (not self.sent) and (self.article_count > 0)):
            new_message = {}
            new_message['articles_list_pickle'] = pickle.dumps(self.articles_list)
            new_message = api.Message(new_message)
            api.send('output', new_message)
            self.sent = True
            api.send('processbar', 'message sent')
        else:
            api.send('processbar', str(self.sent))


w = WordCleaner()


def on_input(message):
    w.get_message(message)
    w.clean()


api.set_port_callback('input', on_input)

api.add_timer('1s', w.check_input)
api.add_timer('1.1s', w.send_message)
