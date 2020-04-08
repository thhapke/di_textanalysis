import io
import json
import os
import re
import subprocess
from datetime import datetime, timezone

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

language = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE', 'Elpais': 'ES',
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
            if port == outports[2]['name']:
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
            version = "0.0.18"
            operator_name = 'metadata_articles'
            operator_description = "Metadata Articles"
            operator_description_long = "Create metadata from articles."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



def process(msg):

    logger, log_stream = slog.set_logging('metadata_articles', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    adict = msg.body
    att_dict = msg.attributes

    metadata_articles = list()
    articles_table = list()

    for index_article, article in enumerate(adict):
        metadata = {'media': article['media'], 'date': article['date'], 'language': language[article['media']], \
                    'hash_text': article['hash_text'], 'url': article['url'][:255], 'rubrics': article['rubrics'],
                    'title': article['title'][:255]}
        metadata['num_characters'] = len(article['text'])
        metadata_articles.append(metadata)
        datea = datetime.strptime(article['date'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
        articles_table.append([article['media'],datea,language[article['media']],article['hash_text'],\
                              article['url'][:255],article['rubrics'],article['title'][:255]])


    table_att = {"columns": [
        {"class": "string", "name": "MEDIA", "nullable": True,  "size": 80, "type": {"hana": "NVARCHAR"}},
        {"class": "string", "name": "DATE", "nullable": False, "type": {"hana": "DATETIME"}},
        {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2, "type": {"hana": "NVARCHAR"}},
        {"class": "string", "name": "HASH_TEXT", "nullable": True, "type": {"hana": "INTEGER"}},
        {"class": "string", "name": "URL", "nullable": True, "size": 255,"type": {"hana": "NVARCHAR"}},
        {"class": "string", "name": "RUBRICS", "nullable": True, "size": 80,"type": {"hana": "NVARCHAR"}},
        {"class": "string", "name": "TITLE", "nullable": True, "size": 255,"type": {"hana": "NVARCHAR"}}],
              "name": "DIPROJECTS.ARTICLES_METADATA2", "version": 1}

    logger.debug('Process ended, articles processed {}  - {}  '.format(len(adict), time_monitor.elapsed_time()))

    att_dict['content'] = 'metadata for articles'
    # JSON
    msg = api.Message(attributes=att_dict, body=metadata_articles)
    api.send(outports[2]['name'], msg)
    # TABLE
    att_dict['table'] = table_att
    msg = api.Message(attributes=att_dict, body=articles_table)
    api.send(outports[1]['name'], msg)

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'table', 'type': 'message.table', "description": "table of metadata arcticles"},
            {'name': 'data', 'type': 'message', "description": "Output metadata of articles"}]

#api.set_port_callback(inports[0]['name'], process)


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


if __name__ == '__main__':
    test_operator()

    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


