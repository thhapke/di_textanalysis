import io
import json
import os
import re


from textblob import TextBlob, Blobber
from textblob_de import TextBlobDE as TextBlobDE
from textblob_fr import PatternTagger as PatternTaggerFR, PatternAnalyzer as PatternAnalyzerFR

import nltk
nltk.download('punkt')

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

language_dict = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE', 'Elpais': 'ES',
                 'Elmundo': 'ES'}

supported_languages = ['EN','DE','FR']

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
                new_filename = os.path.basename(msg.attributes['storage.filename']).split('.')[0] + '-sentiment.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False, indent=4))

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '','textblob':''}
            version = "0.0.1"
            operator_description = "Sentiment Analysis"
            operator_description_long = "Sentiment Analysis using lexicographic approach. "
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

last_msg = None
hash_text_list = list()


def get_article_sentiment(article):
    """
    Extracts sentiment analysis for article.
    @param article: article dictionary (retrieved from the Data Lake)
    @returns: (article_level_polarity, article_level_subjectivity)
    """
    if language_dict[article['media']] == 'DE':
        blob = TextBlobDE(article['text'])
        polarity, subjectivity = (blob.sentiment.polarity, blob.sentiment.subjectivity)
    elif language_dict[article['media']] == 'FR':
        tb = Blobber(pos_tagger=PatternTaggerFR(), analyzer=PatternAnalyzerFR())
        blob = tb(article['text'])
        polarity, subjectivity = blob.sentiment
    else:  # for now defaults to FR (just for PoC)
        blob = TextBlob(article['text'])
        polarity, subjectivity = (blob.sentiment.polarity, blob.sentiment.subjectivity)
    return polarity, subjectivity


def process(msg):
    global setup_data
    global last_msg
    global hash_text_list

    operator_name = 'sentiment analysis'
    logger, log_stream = slog.set_logging(operator_name, loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    if api.config.debug_mode:
        api.send(outports[0]['name'], log_stream.getvalue())

    article_list = msg.body
    att_dict = msg.attributes
    att_dict['operator'] = operator_name

    sentiments_list = list()
    media_set = set()
    for article in article_list:
        media_set.add(article['media'])

        # Ensure that text only analysed once
        if article['hash_text'] in hash_text_list:
            continue
        hash_text_list.append(article['hash_text'])

        if not language_dict[article['media']] in supported_languages :
            continue

        polarity, subjectivity =  get_article_sentiment(article)

        sentiments_list.append({'DATE': article['date'], 'MEDIA': article['media'],'HASH_TEXT': article['hash_text'],\
                                'POLARITY': polarity, 'SUBJECTIVITY': subjectivity})

    logger.debug('Process ended, analysed media: {} - article sentiments analysed {}  - {}'.format(str(media_set), len(sentiments_list),\
                                                                                      time_monitor.elapsed_time()))

    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=sentiments_list))


inports = [{'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary "}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message', "description": "Output List of sentiment records"}]

api.set_port_callback(inports[0]['name'], process)

def main():
    config = api.config
    config.debug_mode = True
    api.set_config(config)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if
                    os.path.isfile(os.path.join(in_dir, f)) and re.match('Spiegel.*', f)]
    for i, fname in enumerate(files_in_dir):
        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir), 'process_list': []}
        with open(os.path.join(in_dir, fname)) as json_file:
            data = json.load(json_file)
        msg_data = api.Message(attributes=attributes, body=data)
        process(msg_data)


