import io
import json
import os
import csv
import re
from datetime import datetime, timedelta
import collections
import nltk

import spacy
nlp_g = spacy.load('de_core_news_sm')
nlp_fr = spacy.load('fr_core_news_sm')
nlp_es = spacy.load('es_core_news_sm')

from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

language_dict = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE', 'Elpais': 'ES',
            'Elmundo': 'ES'}

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                new_filename = msg.attributes['storage.filename']+'words.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False,indent=4))
            else:
                #print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config


        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'spacy': ''}
            version = "0.0.1"
            operator_description = "Text Cleansing"
            operator_description_long = "Prepares text for further processings"
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            language = 'None'
            config_params['language'] = {'title': 'Language','description': 'Filter for language of media.','type': 'string'}


            mode = 'PROPN'
            config_params['mode'] = {'title': 'Mode','description': 'Define the kind of data extraction. \"NOUN\": nouns,'
                                        '\"PNOUN\": proper nouns. \"P+NOUN\": proper nouns and nouns Default is removing stopwords.','type': 'string'}

            counter = True
            config_params['counter'] = {'title': 'Counter','description': 'Returns counter if \"TRUE\" or a list of words.',\
                                        'type': 'boolean'}

            max_word_len = 80
            config_params['max_word_len'] = {'title': 'Maximum word lenght','description': 'Maximum word lenght.','type': 'integer'}


# global articles
setup_data = list()
last_msg = None
hash_list = list()

def setup(msg):
    global setup_data
    setup_data = msg.body
    process(None)

# Checks if corrections has been set
def check_for_setup(logger, msg):
    global setup_data, last_msg

    # Case: Keywords has been set before
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            logger.info("Last msg list has been retrieved")
            return last_msg
    else:
        logger.debug('Processing data received!')
        if last_msg == None:
            last_msg = msg
        else:
            last_msg.attributes = msg.attributes
            last_msg.body.extend(msg.body)
        if len(setup_data) == 0:
            logger.info('No setup message - msg has been saved')
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg


def process(msg):
    global setup_data
    global last_msg
    global hash_list

    logger, log_stream = slog.set_logging('text cleansing', loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    msg = check_for_setup(logger, msg)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    adict = msg.body
    att_dict = msg.attributes

    language_filter = tfp.read_value(api.config.language)
    article_words = dict()
    article_count = 0
    for index_article, article in enumerate(adict):

        language = language_dict[article['media']]

        # filter language
        if language_filter and not language_filter == language :
            #logger.debug('Language filtered out: {} ({})'.format(language, language_filter))
            continue

        article_count += 1
        # check if article has been processed
        if article['hash_text'] in hash_list :
            logger.debug('Article has already been processed: {} - {} - {}'.format(article['date'],article['media'],article['hash_text']))
            continue
        hash_list.append(article['hash_text'])

        text = article['text']
        text = re.sub(r'\d+', '', text.lower())
        text = re.sub(r'\b[a-z]\b', '', text)

        # Language settings
        if language == 'DE':
            doc = nlp_g(text)
        elif language == 'FR':
            doc = nlp_fr(text)
        elif language == 'ES':
            doc = nlp_es(text)
        else :
            logger.warning('Language not implemented')
            doc = None
            words = []

        # only when doc has been created - language exists
        if doc :
            if api.config.mode == 'P+NOUN' :
                words = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_  in ['PROPN', 'NOUN'] ]
            elif api.config.mode == 'NOUN':
                words = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_ == 'NOUN']
            elif api.config.mode == 'PROPN' :
                words = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_ == 'PROPN']
            else :
                words = [token.text[:api.config.max_word_len] for token in doc if not token.is_stop]

        # Remove blacklist words
        words = [ w for w in words if w not in setup_data]

        if api.config.counter:
            article_words[article['hash_text']] = collections.Counter(words)
        else :
            article_words[article['hash_text']] = words


    msg = api.Message(attributes=att_dict,body = article_words)
    logger.info('File processed: {} #Articles: {} '.format(att_dict["storage.filename"],len(adict)))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], msg)


inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},\
            {'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},\
            {'name': 'data', 'type': 'message', "description": "Output List of dicts with word frequency"}]

api.set_port_callback(inports[0]['name'], setup)
api.set_port_callback(inports[1]['name'], process)

def main():
    config = api.config
    config.debug_mode = True
    config.mode = 'NOUN'
    config.language = 'DE'
    config.max_word_len = 80
    config.counter = True

    api.set_config(config)

    bl_filename = '/Users/Shared/data/onlinemedia/repository/blacklist.txt'
    blacklist = list()
    with open(bl_filename,mode='r') as csv_file:
        rows = csv.reader(csv_file,delimiter=',')
        for r in rows :
            blacklist.append(r[0])
        #print(csv_file.read())
    bl_msg = api.Message(attributes={'filename': bl_filename}, body=blacklist)
    setup(bl_msg)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('Spiegel.*',f)]

    for i, fname in enumerate(files_in_dir):

        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        with open(os.path.join(in_dir,fname)) as json_file:
            try :
                data = json.load(json_file)
            except UnicodeDecodeError as e :
                print('Error reading {} ({})'.format(fbase,e))

        print('Read file: {} ({}/{}'.format(fbase,attributes['storage.fileIndex'],attributes['storage.fileCount']))
        msg_data = api.Message(attributes=attributes, body=data)

        process(msg_data)


