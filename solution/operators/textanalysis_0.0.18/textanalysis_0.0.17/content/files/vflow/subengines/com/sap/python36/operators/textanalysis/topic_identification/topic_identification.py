
import csv

#import sklearn
import numpy as np
import pandas as pd
from datetime import date
import json
#from sklearn.decomposition import LatentDirichletAllocation, TruncatedSVD
#from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer

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
            pass

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



def process(db_msg):


    logger, log_stream = slog.set_logging('topic dispatcher', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    columns = [c['name'] for c in db_msg.attributes['table']['columns']]
    df = pd.DataFrame(db_msg.body, columns=columns)

    # groupby and concatenate words
    gdf = df.groupby(['HASH_TEXT'])['WORD'].apply(' '.join)

    # create document-term matrix
    #tf_vectorizer = CountVectorizer(analyzer='word',
    #                                min_df=1,  # minimum reqd occurences of a word
    #                                # stop_words='german',             # remove stop words
    #                                lowercase=False,  # convert all words to lowercase
    #                                # token_pattern='[a-zA-Z0-9]{1,}',  # num chars > 3
    #                                # max_features=5000,             # max number of uniq words
    #                                )

    # tf means term-frequency in a document
    #dtm_tf = tf_vectorizer.fit_transform(gdf)
    # for tf dtm
    #lda_tf = LatentDirichletAllocation(n_components=30, learning_method='online', evaluate_every=-1, n_jobs=-1)
    #lda_tf.fit(dtm_tf)

    # get the first 10 keywords of each topic
    # get the words
    #feature_names = tf_vectorizer.get_feature_names()
    # topics can be extracted from components_
#    date_today = str(date.today()) + '-'

#    for topic_ii, topic in enumerate(lda_tf.components_):
#        topic_id = str(date.today()) + '-' + str(topic_ii)
#        language = 'DE'
#        topic_type = 'LDA'
#        topic_date = str(date.today())
#        experiy_date = None
#        attribute = None
#        topic_words = [feature_names[ii] for ii in topic.argsort()[:-11:-1]]
#        row = [topic_id, language, topic_type, topic_date, experiy_date, attribute]
#        row.extend(topic_words)
#        topic_list.append(row)

    #topic_np = np.array(topic_list, dtype='object')
    #col_names = ['TOPIC', 'LANGUAGE', 'TYPE', 'DATE', 'EXPERIY_DATE', 'ATTRIBUTE']
    #for ii in range(1, 11):
    #    col_names.append(f'KEYWORD_{ii}')

    #self.topic_df = pd.DataFrame(topic_np, columns=col_names)


    logger.debug('Process ended, topics processed {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())


outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'sql', 'type': 'message', "description": "message with body is sql and attributes contains the topic"}]
inports = [{'name': 'topic', 'type': 'message.table', "description": "Message with body as table."}]


api.set_port_callback(inports[0]['name'], process)


def main():
    word_filename = '/Users/Shared/data/onlinemedia/repository/word_index2.csv'
    words = list()
    with open(word_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        next(rows,None)
        for r in rows:
            words.append([r[0],r[1],r[2]])

    attributes = {"table":{"columns":[{"class":"string","name":"HASH_TEXT","nullable":True,"type":{"hana":"INTEGER"}},
                                      {"class":"string","name":"WORD","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"COUNT","nullable":True,"type":{"hana":"INTEGER"}}],
                           "name":"DIPROJECTS.WORD_INDEX2","version":1}}

    word_msg = api.Message(attributes=attributes, body=words)
    process(word_msg)

