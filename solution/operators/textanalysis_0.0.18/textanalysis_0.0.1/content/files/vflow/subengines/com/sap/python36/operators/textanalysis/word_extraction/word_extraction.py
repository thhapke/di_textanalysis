import io
import json
import os
import csv
import re
import pickle
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

# global variable settings
language_dict = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE', 'Elpais': 'ES',
            'Elmundo': 'ES'}
supported_modes = 'PNX'

try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                #wi_fn = os.path.join('/Users/Shared/data/onlinemedia/repository',msg.attributes['storage.filename']+'-words.csv')
                #with open(wi_fn, 'w') as f:
                #    writer = csv.writer(f)
                #    cols = [c['name'] for c in msg.attributes['table']['columns']]
                #    writer.writerow(cols)
                #    writer.writerows(msg.body)
                api.queue.append(msg)
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
            operator_description = "Word Extraction"
            operator_description_long = "Extracts words from text for further analysis."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            use_blacklist = True
            config_params['use_blacklist'] = {'title': 'Use blacklist',
                                           'description': 'Use the blacklist for omitting words.',
                                           'type': 'boolean'}

            language = 'None'
            config_params['language'] = {'title': 'Language','description': 'Filter for language of media.','type': 'string'}


            mode = 'PN'
            config_params['mode'] = {'title': 'Mode','description': 'Define the kind of data extraction. P=Proper Nouns, '
                                     'N=Nouns, X: Removing only stopwords.','type': 'string'}

            counter = True
            config_params['counter'] = {'title': 'Counter','description': 'Returns counter if \"TRUE\" or a list of words.',\
                                        'type': 'boolean'}

            max_word_len = 80
            config_params['max_word_len'] = {'title': 'Maximum word lenght','description': 'Maximum word length.','type': 'integer'}

            min_word_len = 2
            config_params['min_word_len'] = {'title': 'Minimum word length', 'description': 'Minimum word length.','type': 'integer'}

# global articles
blacklist = list()
last_msg = None
hash_list = list()

def setup(msg):
    global blacklist
    blacklist = msg.body
    process(None)

# Checks if corrections has been set
def check_for_setup(logger, msg, mode = True):
    global blacklist, last_msg

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
        # saving of data msg
        if last_msg == None:
            last_msg = msg
        else:
            last_msg.attributes = msg.attributes
            last_msg.body.extend(msg.body)

        # check if data msg should be returned or none if setup is not been done
        if len(blacklist) == 0 and mode == True:
            logger.info('No setup message - msg has been saved')
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg


def process(msg):
    global blacklist
    global last_msg
    global hash_list

    logger, log_stream = slog.set_logging('word_extraction', loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    msg = check_for_setup(logger, msg,mode = api.config.use_blacklist)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    adict = msg.body

    language_filter = tfp.read_value(api.config.language)
    mode = tfp.read_value(api.config.mode)

    if not mode or not any(m in mode for m in supported_modes) :
        raise Exception('Mode is mandatory parameter and valid values are: {}'.format(supported_modes))

    article_words = list()
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
        # might interfer with
        #text = re.sub(r'\d+', '', text.lower())
        #text = re.sub(r'\b[a-z]\b', '', text)

        # Language settings
        if language == 'DE':
            doc = nlp_g(text)
        elif language == 'FR':
            doc = nlp_fr(text)
        elif language == 'ES':
            doc = nlp_es(text)
        else :
            logger.warning('Language not implemented: {}'.format(language))
            doc = None

        words = dict()
        # only when doc has been created - language exists
        if doc :
            if 'P' in api.config.mode  :
                words['P'] = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_   == 'PROPN' ]
            if 'N' in api.config.mode :
                words['N'] = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_ == 'NOUN']
            if 'X' in api.config.mode :
                words['X'] = [token.text[:api.config.max_word_len] for token in doc if not token.is_stop]

        for m in words:
            # heuristics

            # remove preceding non-alpha characters
            words[m] = [re.sub('^[-\'\./]', '', w) for w in words[m] ]
            # remove trailing non-alpha characters
            words[m] = [re.sub('[-\./]$', '', w) for w in words[m]]
            # minimum word length
            words[m] = [w for w in words[m] if len(w) >= api.config.min_word_len]
            # remove date like words
            words[m] = [w for w in words[m] if not re.findall('\d+\.\d+',w)]
            # Remove blacklist words
            if api.config.use_blacklist :
                words[m] = [w for w in words[m] if w not in blacklist]
            if api.config.counter:
                article_words.append([article['hash_text'],language,m,collections.Counter(words[m]).most_common()])
            else :
                article_words.append([article['hash_text'],language,m,words[m]])


    attributes = {
            "table": {"columns": [{"class": "string", "name": "HASH_TEXT", "nullable": True, "type": {"hana": "INTEGER"}},
                              {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2,"type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "TYPE", "nullable": True, "size": 1,"type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "WORDS", "nullable": True, "type": {"hana": "ARRAY"}}],
                  "name": "DIPROJECTS.WORD_INDEX3", "version": 1},
        "storage.filename" : msg.attributes["storage.filename"]}


    attributes['counter'] = 'Y' if api.config.counter else 'N'

    table_msg = api.Message(attributes=attributes,body = article_words)
    logger.info('File processed: {} #Articles: {} '.format(msg.attributes["storage.filename"],len(adict)))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], table_msg)


inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},\
            {'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},\
            {'name': 'data', 'type': 'message.table', "description": "Output List of dicts with word frequency"}]

api.set_port_callback(inports[0]['name'], setup)
api.set_port_callback(inports[1]['name'], process)

def test_operator():
    config = api.config
    config.debug_mode = True
    config.mode = 'PN'
    config.language = 'DE'
    config.max_word_len = 80
    config.counter = True
    config.use_blacklist = True
    config.min_word_len = 2

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
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*json',f)]

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

        # saving outcome as word index
        word_type_counter = { t:collections.Counter() for t in supported_modes}
        for m in api.queue :
            for article in m.body:
                wdict = { w: c for w,c in article[3]}
                word_type_counter[article[2]].update(collections.Counter(wdict))

        with open('/Users/Shared/data/onlinemedia/repository/word_extraction.csv', 'w') as f:
            writer = csv.writer(f)
            cols = ['type','word','count']
            writer.writerow(cols)
            for t in word_type_counter :
                mc = word_type_counter[t].most_common()
                for w,c in mc :
                    writer.writerow([t,w,c])


        with open('/Users/Shared/data/onlinemedia/repository/word_extraction.pickle', 'wb') as f:
            pickle.dump(api.queue, f)


