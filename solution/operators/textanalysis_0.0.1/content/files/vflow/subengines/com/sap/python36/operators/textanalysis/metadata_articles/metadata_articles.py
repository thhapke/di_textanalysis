import io
import json
import os
import re
import subprocess

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

language = {'Lefigaro': 'F', 'Lemonde': 'F', 'Spiegel': 'G', 'FAZ': 'G', 'Sueddeutsche': 'G', 'Elpais': 'S',
            'Elmundo': 'S'}

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
                new_filename = os.path.basename(msg.attributes['storage.filename']).split('.')[0] + '-metadata.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False))

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'nltk': ''}
            version = "0.0.1"
            operator_description = "Metadata Articles"
            operator_description_long = "Create metadata from articles."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


hash_dict = dict()

def process(msg):
    global hash_dict

    logger, log_stream = slog.set_logging('metadata_articles', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    adict = msg.body
    att_dict = msg.attributes

    metadata_articles = list()

    for index_article, article in enumerate(adict):
        metadata = {'media': article['media'], 'date': article['date'], 'language': language[article['media']], \
                    'hash_text': article['hash_text'], 'url': article['url'][:255], 'rubrics': article['rubrics'],
                    'title': article['title'][:255]}
        metadata['num_characters'] = len(article['text'])
        metadata_articles.append(metadata)
        #test if key already exist
        #if not metadata['hash_text'] in hash_dict :
        #    hash_dict[metadata['hash_text']] = [metadata['date']]
        #    metadata_articles.append(metadata)
        #else :
        #    if metadata['date'] in hash_dict[metadata['hash_text']] :
        #        logger.warning('Hash_text and date exist already: {} - {} - {} - {}'.\
        #                       format(metadata['date'], metadata['media'],  metadata['title'],metadata['hash_text']))
        #    else :
        #        hash_dict[metadata['hash_text']].append(metadata['date'])
        #        metadata_articles.append(metadata)

    logger.debug('Process ended, articles processed {}  - {}  '.format(len(adict), time_monitor.elapsed_time()))

    att_dict['content'] = 'metadata for articles'
    msg = api.Message(attributes=att_dict, body=metadata_articles)
    api.send(outports[1]['name'], msg)
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'data', 'type': 'message', "description": "Output metadata of articles"}]

api.set_port_callback(inports[0]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.num_important_words = 100
    api.set_config(config)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*json', f)]

    for i, fname in enumerate(files_in_dir):

        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        with open(os.path.join(in_dir,fname)) as json_file:
            data = json.load(json_file)
        msg_data = api.Message(attributes=attributes, body=data)

        process(msg_data)


