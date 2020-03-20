import io
import json
import os
import csv
import re

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp


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
                new_filename = 'word_index.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False,indent=4))

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.0.1"
            operator_description = "Topic dispatcher"
            operator_description_long = "Sends input topics to SQL processor and topic frequency operator."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):

    logger, log_stream = slog.set_logging('topic dispatcher', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    articles = msg.body
    word_index = list()
    for hash_text, words in articles.items() :
        word_index.extend([{'hash_text':hash_text,'word':w,'count':words[w]} for w in words ])

    logger.debug('Process ended, articles processed {}  - {}  '.format(len(articles), time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())

    msg = api.Message(attributes={"content": "word_index"},body=word_index)
    api.send(outports[1]['name'], msg)


outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'words', 'type': 'message', "description": "message with body is dictionary"}]
inports = [{'name': 'articles', 'type': 'message', "description": "Message with list of words"}]

api.set_port_callback(inports[0]['name'], process)


def main():
    in_dir = '/Users/Shared/data/onlinemedia/prep_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if
                    os.path.isfile(os.path.join(in_dir, f)) and re.match('.*words.json', f)]

    for i, fname in enumerate(files_in_dir):

        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir), 'process_list': []}
        with open(os.path.join(in_dir, fname)) as json_file:
            try:
                data = json.load(json_file)
            except UnicodeDecodeError as e:
                print('Error reading {} ({})'.format(fbase, e))

        print('Read file: {} ({}/{}'.format(fbase, attributes['storage.fileIndex'], attributes['storage.fileCount']))
        msg_data = api.Message(attributes=attributes, body=data)

        process(msg_data)


