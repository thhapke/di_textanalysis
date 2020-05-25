import json
import os
import csv
import re
import pandas as pd
import numpy as np
import pickle
import collections
import subprocess


import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

pd.set_option('mode.chained_assignment','warn')

try:
    api
except NameError:
    class api:

        queue_d = list()
        queue_r = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[2]['name'] :
                api.queue_d.append(msg)
            elif port == outports[1]['name'] :
                api.queue_r.append(msg)
            elif  port == outports[0]['name'] :
                #print(msg)
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '','pandas': ''}
            version = "0.0.18"
            operator_name = "word_regex"
            operator_description = "Regex on Words"
            operator_description_long = "Run regex on words."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            word_types = 'PROPN'
            config_params['word_types'] = {'title': 'Word types',
                                           'description': 'Setting word type selection for delete',
                                           'type': 'string'}

            language_filter = 'None'
            config_params['language_filter'] = {'title': 'Language filter', 'description': 'Filter for languages of media.',
                                         'type': 'string'}

            pattern_word_removal = None
            config_params['pattern_word_removal'] = {'title': 'Regular expression patterns for removing the word',
                                           'description': 'Regular expression patterns for removing the word',
                                           'type': 'string'}

            pattern_substring_replace = None
            config_params['pattern_substring_replace'] = {'title': 'Regular expression patterns for replacing substring of words',
                                           'description': 'Regular expression patterns for replacing substring of words',
                                           'type': 'string'}

def process(msg):

    att_dict = msg.attributes

    logger, log_stream = slog.set_logging('word_regex', api.config.debug_mode)
    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    # Dataframe
    df = msg.body
    df['word_m'] = np.nan
    df['WORD_R'] = np.nan

    # word type
    word_types = tfp.read_list(api.config.word_types)
    if not word_types :
        word_types = list(df['type'].unique())

    # Language filter
    language_filter = tfp.read_list(api.config.language_filter)
    if not language_filter :
        language_filter = list(df['language'].unique())

    mask = df['language'].isin(language_filter) & df['type'].isin(word_types)

    # regex patterns word removal
    regex_wordr = tfp.read_list(api.config.pattern_word_removal)
    remove_words = list()
    if regex_wordr :
        for ipat, pat in enumerate(regex_wordr):
            logger.info('Execute pattern: {} ({}/{})'.format(pat,ipat,len(regex_wordr)))
            api.send(outports[0]['name'], log_stream.getvalue())
            log_stream.truncate()
            log_stream.seek(0)
            df.loc[mask & df['word'].str.contains(pat = pat),'word_r'] = pat

    # regex patterns word removal
    regex_ssr = tfp.read_dict(api.config.pattern_substring_replace)
    if regex_ssr :
        for ipat, pat in enumerate(regex_ssr.items()):
            logger.info('Execute replace pattern: {} ({}/{})'.format(pat,ipat,len(regex_ssr)))
            api.send(outports[0]['name'], log_stream.getvalue())
            log_stream.truncate()
            log_stream.seek(0)
            df.loc[mask & df['word'].str.contains(pat=pat[0]), 'word_m'] = pat[0]
            df.loc[mask,'word'] = df.loc[mask,'word'].str.replace(pat[0],pat[1],regex = True)

    r_df= df.loc[df[['word_r','word_m']].any(axis=1),['word','word_r','word_m']].drop_duplicates()
    df.drop(df.loc[~df['word_r'].isnull()].index,axis = 0, inplace = True)
    df.drop(columns=['word_r','word_m'],inplace = True)

    # test for duplicates
    dup_s = df.duplicated(subset=['text_id','language','type','word']).value_counts()
    num_duplicates = dup_s[True] if True in dup_s  else 0
    logger.info('Duplicates: {} / {}'.format(num_duplicates, df.shape[0]))


    api.send(outports[1]['name'], api.Message(attributes={'Type':'Removed Words','Format':'list'}, body=r_df))
    api.send(outports[2]['name'], api.Message(attributes=att_dict, body=df))
    api.send(outports[0]['name'],log_stream.getvalue())


inports = [{'name': 'words', 'type': 'message.DataFrame', "description": "Message table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},\
            {'name': 'removed', 'type': 'message.DataFrame', "description": "Removed words"},          \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Table after regex"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():

    config = api.config
    config.debug_mode = True
    config.pattern_word_removal = 'None'
    config.pattern_word_removal = '"\d+", "["\(\)""]", "^[-\'\./+]", "[-\./+@]$"'
    config.pattern_substring_replace = '[\.@!?] : " "'
    config.test_mode = False
    config.word_type = 'PROPN'
    config.language_filter = 'DE'
    api.set_config(config)

    doc_file = '/Users/Shared/data/onlinemedia/data/word_extraction.csv'
    df = pd.read_csv(doc_file,sep=',',nrows=10000000)
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df)
    process(msg)

    # saving outcome as word index
    out_file = '/Users/Shared/data/onlinemedia/data/word_extraction_regex.csv'
    df_list = [d.body for d in api.queue_d]
    pd.concat(df_list).to_csv(out_file, index=False)

    out_file = '/Users/Shared/data/onlinemedia/data/word_regex_removed.csv'
    df_list = [d.body for d in api.queue_r]
    pd.concat(df_list).to_csv(out_file, index=False)

if __name__ == '__main__':
    #test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])




