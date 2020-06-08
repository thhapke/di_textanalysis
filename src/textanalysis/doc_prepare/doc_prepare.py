import json
import os
import csv
import re
import pandas as pd
import pickle
import collections
import subprocess

import spacy

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp


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
            tags = {'sdi_utils': ''}
            version = "0.1.0"
            operator_name = "doc_prepare"
            operator_description = "Doc Prepare"
            operator_description_long = "Prepares documents read from storage by select the values and remove formats."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            text_column = 'text'
            config_params['text_column'] = {'title': 'TEXT Column',
                                           'description': 'Name of incoming text column',
                                           'type': 'string'}

            id_column = 'text_id'
            config_params['id_column'] = {'title': 'ID Column',
                                           'description': 'Name of incoming id column',
                                           'type': 'string'}

            language_column = 'language'
            config_params['language_column'] = {'title': 'Name of language column',
                                           'description': 'Name of language column in input data.',
                                           'type': 'string'}

            default_language = 'DE'
            config_params['default_language'] = {'title': 'Default language',
                                           'description': 'Default language of the texts',
                                           'type': 'string'}

            remove_html_tags =  False
            config_params['remove_html_tags'] = {'title': 'Remove html tags',
                                           'description': 'Remove html tags from text',
                                           'type': 'boolean'}

            media_docs =  False
            config_params['media_docs'] = {'title': 'Media articles',
                                           'description': 'Name of media article determines language.',
                                           'type': 'boolean'}

ID_set = set()
def process(msg):

    global ID_list

    operator_name = 'doc_prepare'
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att_dict = msg.attributes
    df = msg.body

    text_column = tfp.read_value(api.config.text_column)
    if not text_column :
        text_column = 'text'

    id_column = tfp.read_value(api.config.id_column)
    if not id_column:
        id_column = 'text_id'

    default_language = 'DE'
    if api.config.media_docs :
        language_column = 'language'
        df['language'] = default_language
        df.loc[df['media'].isin(['Lefigaro','Lemonde']),'language'] = 'FR'
        df.loc[df['media'].isin(['Elpais', 'Elmundo']), 'language'] = 'ES'
    else :
        language_column = tfp.read_value(api.config.language_column)
        if not language_column :
            language_column = 'language'

        r_default_language = tfp.read_value(api.config.default_language)
        if r_default_language :
            default_language = r_default_language

        if not language_column in df.columns :
            df[language_column] = default_language
        else :
            df.loc[df['language'].isna()] = default_language

    df.rename(columns={text_column: 'text', id_column: 'text_id', language_column:'language'}, inplace=True)
    logger.debug('Columns: {}'.format(df.columns))

    logger.info("Default language: {}".format(default_language))


    # remove duplicates
    prev_num_rows = df.shape[0]
    df.drop_duplicates(subset = ['text_id'],inplace = True)
    df = df.loc[~df['text_id'].isin(ID_set)]
    post_num_rows = df.shape[0]
    logger.debug('Docs reduced due to be already processed: {} - {}'.format(prev_num_rows, post_num_rows))
    ID_set.update(df.text_id.values.tolist())

    # replace html tags
    if api.config.remove_html_tags :
        df['text'] = df['text'].str.replace('<.*?>','',regex=True)

    # replace double double quotes
    df['text'] = df['text'].str.replace('""', '', regex=False)
    df['text'] = df['text'].str.replace('."', '. "', regex=False)

    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict,body=df[['text_id','language','text']]))


inports = [{'name': 'docs', 'type': 'message.DataFrame', "description": "Message table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Table with word index"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():
    global labels

    config = api.config
    config.debug_mode = True
    config.text_column = 'ARTIFACT_CONTENT.TEXT'
    config.id_column = 'ID'
    config.default_language = 'DE'
    api.set_config(config)

    doc_file = '/Users/Shared/data/onlinemedia/data/print_example.csv'
    df = pd.read_csv(doc_file,sep='\t',nrows=100000000)
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df)
    process(msg)

    n_df = df.head(10).copy()
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=n_df)
    process(msg)

    # saving outcome as word index
    out_file = '/Users/Shared/data/onlinemedia/data/doc_data_cleansed.csv'
    df_list = [d.body for d in api.queue]
    pd.concat(df_list).to_csv(out_file,index=False)


if __name__ == '__main__':
    test_operator()

    if True :
        solution_name = api.config.operator_name + '_' + api.config.version
        path = os.path.join('/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/',solution_name)
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


