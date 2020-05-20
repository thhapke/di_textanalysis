import json
import os
import csv
import pandas as pd
import re
import pickle
import collections
import subprocess

import spacy

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp


supported_languages = ['DE', 'EN', 'ES', 'FR']
lexicon_languages = {lang: False for lang in supported_languages}

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
                api.queue.append(msg)
            else:
                # print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'pandas': ''}
            version = "0.0.18"
            operator_name = "word_lexicon"
            operator_description = "Lexicon Words"
            operator_description_long = "Map words according to lexicon."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            language_filter = 'None'
            config_params['language_filter'] = {'title': 'Language Filter', 'description': 'Filter for language of media.',
                                         'type': 'string'}

            word_types = 'PROPN'
            config_params['word_types'] = {'title': 'Type',
                                     'description': 'Setting word type selection for mapping.', 'type': 'string'}

# global articles
lexicon = pd.DataFrame()
last_msg = None
id_set = list()
operator_name = 'word_lexicon'


def setup_lexicon(msg):
    global lexicon
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    logger.info('Set lexicon')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    lexicon = msg.body
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)


# Checks for setup
def check_for_setup(logger, msg) :
    global lexicon, last_msg

    logger.info("Check setup")
    # Case: setupdate, check if all has been set
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            if  lexicon == None  :
                logger.info("Setup not complete -  lexicon: {}".format( len(lexicon)))
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
        if lexicon.empty:
            len_lex = 0 if lexicon == None else len(lexicon)
            logger.info("Setup not complete - lexicon: {}".format(len_lex))
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg


def process(msg):
    global last_msg
    global id_set
    global lexicon

    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    # Check if setup complete
    msg = check_for_setup(logger, msg)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att_dict = msg.attributes

    df = msg.body

    # word type
    word_types = tfp.read_list(api.config.word_types)
    if not word_types :
        word_types = list(df['TYPE'].unique())

    # Language filter
    language_filter = tfp.read_list(api.config.language_filter)
    if not language_filter :
        language_filter = list(df['LANGUAGE'].unique())

    mask = df['TYPE'].isin(word_types) & df['LANGUAGE'].isin(language_filter)

    lex_list = list()
    for lang in lexicon:
        words_row = lexicon.loc[(lexicon['MAPPING']=='M') & lexicon['LANGUAGE'].isin(language_filter)]
        for index, row in words_row.iterrows():
            new_df = df.loc[mask & df['WORD'].str.match(row['WORD'])].copy()
            new_df['TYPE'] = 'LEX'
            new_df['WORD'] = row['CATEGORY']
            lex_list.append(new_df)
        words_row = lexicon.loc[(lexicon['MAPPING']=='FM') & lexicon['LANGUAGE'].isin(language_filter)]
        for index, row in words_row.iterrows():
            new_df = df.loc[mask & (df['WORD'] == row['WORD'])].copy()
            new_df['TYPE'] = 'LEX'
            new_df['WORD'] = row['CATEGORY']
            lex_list.append(new_df)
        words_row = lexicon.loc[(lexicon['MAPPING']=='C') & lexicon['LANGUAGE'].isin(language_filter)]
        for index, row in words_row.iterrows():
            new_df = df.loc[mask & df['WORD'].str.contains(row['WORD'])].copy()
            new_df['TYPE'] = 'LEX'
            new_df['WORD'] = row['CATEGORY']
            lex_list.append(new_df)
        lex_list.append(new_df)
    new_df = pd.concat(lex_list)
    df = df.append(new_df,ignore_index = True)


    df = df.groupby(by=['ID','LANGUAGE','TYPE','WORD'])['COUNT'].sum().reset_index()

    # test for duplicates
    dup_s = df.duplicated(subset=['ID','LANGUAGE','TYPE','WORD']).value_counts()
    num_duplicates = dup_s[True] if True in dup_s  else 0
    logger.info('Duplicates: {} / {}'.format(num_duplicates, df.shape[0]))

    #df_l = df.loc[df['TYPE'] =='LEX']
    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=df))
    api.send(outports[0]['name'],log_stream.getvalue())


inports = [{'name': 'lexicon', 'type': 'message.DataFrame', "description": "Message with body as  DataFrame."},
           {'name': 'table', 'type': 'message.DataFrame', "description": "Message with body as  DataFrame."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Data after mapping lexicon"}]

#api.set_port_callback(inports[0]['name'], setup_lexicon)
#api.set_port_callback(inports[1]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.type = 'P'
    config.new_type = 'P'
    config.language = 'None'
    api.set_config(config)

    # LEXICON
    lex_filename = '/Users/Shared/data/onlinemedia/repository/lexicon.csv'
    df = pd.read_csv(lex_filename,sep=',',nrows=10000000)
    msg = api.Message(attributes={'format':'DataFrame'}, body=df)
    setup_lexicon(msg)

    word_index_filename = '/Users/Shared/data/onlinemedia/data/word_extraction_regex.csv'
    attributes = {'format':'DataFrame'}
    df = pd.read_csv(word_index_filename,sep=',',nrows=10000000)
    msg = api.Message(attributes={'file': {'path': word_index_filename},'format':'pandas'}, body=df)
    process(msg)


    # saving outcome as word index
    with open('/Users/Shared/data/onlinemedia/data/word_extraction_regex_lexicon.csv', 'w') as f:
        writer = csv.writer(f)
        cols = ['HASH_TEXT', 'LANGUAGE', 'TYPE', 'WORD', 'COUNT']
        writer.writerow(cols)
        for msg in api.queue:
            for rec in msg.body:
                writer.writerow(rec)


if __name__ == '__main__':
    #test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


