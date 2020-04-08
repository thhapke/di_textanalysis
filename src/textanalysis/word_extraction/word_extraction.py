import json
import os
import csv
import re
import pickle
import collections
import subprocess

import spacy

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

# global variable settings
media_languages = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE',
                   'Elpais': 'ES',
                   'Elmundo': 'ES'}


supported_languages = ['DE', 'EN', 'ES', 'FR']
lexicon_languages = {lang: False for lang in supported_languages}
supported_word_types = 'PNXLK'

# ensure the models are downloaded in the first place by 'python -m spacy download <language_model>'
language_models = {'EN': 'en_core_web_sm', 'DE': 'de_core_news_sm', 'FR': 'fr_core_news_sm', 'ES': 'es_core_news_sm',
                   'IT': 'it_core_news_sm', 'LT': 'lt_core_news_sm', 'NB': 'nb_core_news_sm', 'nl': 'nl_core_news_sm',
                   'PT': 'pt_core_news_sm'}
nlp = dict()


def nlp_doc(logger, language_code, text):
    global nlp
    if language_code in language_models.keys():
        if not language_code in nlp.keys():
            nlp[language_code] = spacy.load(language_models[language_code])
            logger.info('Spacy language package loaded: {}'.format(language_models[language_code]))
    else:
        logger.warning('No language model available for: {}'.format(language_code))
        return None
    return nlp[language_code](text)


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
                # wi_fn = os.path.join('/Users/Shared/data/onlinemedia/repository',msg.attributes['storage.filename']+'-words.csv')
                # with open(wi_fn, 'w') as f:
                #    writer = csv.writer(f)
                #    cols = [c['name'] for c in msg.attributes['table']['columns']]
                #    writer.writerow(cols)
                #    writer.writerows(msg.body)
                api.queue.append(msg)
            else:
                # print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'spacy': ''}
            version = "0.0.18"
            operator_name = "word_extraction"
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
            config_params['language'] = {'title': 'Language', 'description': 'Filter for language of media.',
                                         'type': 'string'}

            mode = 'PN'
            config_params['mode'] = {'title': 'Mode',
                                     'description': 'Define the kind of data extraction. P=Proper Nouns, '
                                                    'N=Nouns, X: Removing only stopwords.', 'type': 'string'}

            max_word_len = 80
            config_params['max_word_len'] = {'title': 'Maximum word lenght', 'description': 'Maximum word length.',
                                             'type': 'integer'}

            min_word_len = 3
            config_params['min_word_len'] = {'title': 'Minimum word length', 'description': 'Minimum word length.',
                                             'type': 'integer'}

# global articles
blacklist = list()
keywords = list()
lexicon = None
lexicon_stem = None
last_msg = None
hash_list = list()
operator_name = 'word_extraction'


def setup_blacklist(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global blacklist
    logger.info('Set blacklist')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    blacklist = msg.body
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)

def setup_keywords(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global keywords
    logger.info('Set keywords')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    keywords = msg.body
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)



def setup_lexicon(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global lexicon, lexicon_languages, lexicon_stem
    logger.info('Set lexicon')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    try:
        header = [c["name"] for c in msg.attributes['table']['columns']]
    except Exception as e:
        logger.error(e)
        api.send(outports[0]['name'], log_stream.getvalue())
        return None

    lexicon = {c: dict() for c in header[1:]}
    lexicon_stem = {c: dict() for c in header[1:]}
    for r in msg.body:
        for i, lang in enumerate(header[1:]):
            lang_words = r[i + 1].split()
            lw = list()
            lws = list()
            for w in lang_words :
                if w[-1] == '*' :
                    lws.append(w[:-1])
                else :
                    lw.append(w)
            if lw :
                lw_dict = dict.fromkeys(lw, r[0])
                lexicon[lang].update(lw_dict)
            if lws :
                lws_dict = dict.fromkeys(lws, r[0])
                lexicon_stem[lang].update(lws_dict)

    for lang in header[1:]:
        lexicon_languages[lang] = True
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)


# Checks for setup
def check_for_setup(logger, msg, mode, use_blacklist = False) :
    global blacklist, keywords, lexicon, last_msg

    use_keywords = True if 'K' in mode  else False
    use_lexicon = True if 'L' in mode else False

    logger.info("Check setup")
    # Case: setupdate, check if all has been set
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            if len(blacklist) == 0 or lexicon == None or len(keywords) == 0  :
                logger.info("Setup not complete -  blacklist: {} keywords: {}  lexicon: {}".\
                            format(len(blacklist, len(keywords), len(lexicon))))
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
        if (len(blacklist) == 0  and use_blacklist == True) or \
            (len(keywords) == 0 and  use_keywords == True) or \
            (lexicon == None and use_lexicon == True):
            len_lex = 0 if lexicon == None else len(lexicon)
            logger.info("Setup not complete -  blacklist: {}  keywords: {}  lexicon: {}".\
                        format(len(blacklist), len(keywords), len_lex))
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

    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    # Check if setup complete
    msg = check_for_setup(logger, msg, mode=api.config.mode, use_blacklist=api.config.use_blacklist)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    adict = msg.body

    language_filter = tfp.read_value(api.config.language)
    mode = tfp.read_value(api.config.mode)

    if not mode or not any(m in mode for m in supported_word_types):
        raise Exception('Mode is mandatory parameter and valid values are: {}'.format(supported_word_types))

    use_keywords = True if 'K' in mode  else False
    use_lexicon = True if 'L' in mode else False
    use_blacklist = api.config.use_blacklist

    logger.info('Usage:  keywords: {}  lexicon: {}  blacklist: {} '.format(use_keywords, use_lexicon, use_blacklist))

    article_words = list()
    article_count = 0
    for index_article, article in enumerate(adict):

        language = media_languages[article['media']]

        # filter language
        if language_filter and not language_filter == language:
            # logger.debug('Language filtered out: {} ({})'.format(language, language_filter))
            continue

        article_count += 1
        # check if article has been processed
        if article['hash_text'] in hash_list:
            logger.debug('Article has already been processed: {} - {} - {}'.format(article['date'], article['media'],
                                                                                   article['hash_text']))
            continue
        hash_list.append(article['hash_text'])

        doc = nlp_doc(logger, language, article['text'])

        words = dict()
        # only when doc has been created - language exists
        if doc:
            if 'P' in api.config.mode:
                words['P'] = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_ == 'PROPN']
            if 'N' in api.config.mode:
                words['N'] = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_ == 'NOUN']
            if 'X' in api.config.mode:
                words['X'] = [token.text[:api.config.max_word_len] for token in doc if not token.is_stop]
            if use_keywords :
                #words['K'] = [token.lemma_ for kw in keywords for token in doc if re.match(kw, token.lemma_)]
                words['K'] = [token.lemma_ for kw in keywords for token in doc if kw == token.lemma]
            if use_lexicon and lexicon_languages[language]:
                words['L'] = [lexicon_stem[language][lw] for lw in lexicon_stem[language] for token in doc if re.match(lw, token.lemma_)]
                words['L'] = [lexicon[language][lw] for lw in lexicon[language] for token in doc if lw == token.lemma]

        for m in words:
            # heuristics
            # remove preceding non-alpha characters
            words[m] = [re.sub('^[-\'\./+]', '', w) for w in words[m]]
            # remove trailing non-alpha characters
            words[m] = [re.sub('[-\./+]$', '', w) for w in words[m]]
            # minimum word length
            words[m] = [w for w in words[m] if len(w) >= api.config.min_word_len]
            # remove numbers and dates
            words[m] = [w for w in words[m] if not (re.findall('\d+[\.,]\d+', w) or re.findall('^\d+$', w))]
            # Remove blacklist words
            if use_blacklist:
                words[m] = [w for w in words[m] if w not in blacklist]
            article_words.append([article['hash_text'], language, m, collections.Counter(words[m]).most_common()])


    attributes = {
        "table": {"columns": [{"class": "string", "name": "HASH_TEXT", "nullable": True, "type": {"hana": "INTEGER"}},
                              {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2,
                               "type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "TYPE", "nullable": True, "size": 1,
                               "type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "WORDS", "nullable": True, "type": {"hana": "ARRAY"}}],
                  "name": "DIPROJECTS.WORD_INDEX", "version": 1},
        "storage.filename": msg.attributes["storage.filename"]}

    attributes['counter'] = 'Y' if api.config.counter else 'N'

    table_msg = api.Message(attributes=attributes, body=article_words)
    logger.info('File processed: {} #Articles: {} '.format(msg.attributes["storage.filename"], len(adict)))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], table_msg)


inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},
           {'name': 'keywords', 'type': 'message.list', "description": "Message with keywords as dictionary."},\
           {'name': 'lexicon', 'type': 'message.table', "description": "Message with body as lexicon."}, \
           {'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "Output List of dicts with word frequency"}]

#api.set_port_callback(inports[0]['name'], setup_blacklist)
#api.set_port_callback(inports[1]['name'], setup_keywords)
#api.set_port_callback(inports[2]['name'], setup_lexicon)
#api.set_port_callback(inports[3]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.mode = 'L'
    config.language = 'None'
    config.max_word_len = 80
    config.counter = True
    config.use_blacklist = True
    config.min_word_len = 3

    api.set_config(config)


    # BLACKLIST
    bl_filename = '/Users/Shared/data/onlinemedia/repository/blacklist.txt'
    blacklist = list()
    with open(bl_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        for r in rows:
            blacklist.append(r[0])
        # print(csv_file.read())
    bl_msg = api.Message(attributes={'filename': bl_filename}, body=blacklist)
    setup_blacklist(bl_msg)

    # KEYWORDS
    kw_filename = '/Users/Shared/data/onlinemedia/repository/keywords.txt'
    keywords = list()
    with open(kw_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        for r in rows:
            keywords.append(r[0])
        # print(csv_file.read())
    kw_msg = api.Message(attributes={'filename': kw_filename}, body=keywords)
    setup_keywords(kw_msg)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('Spieg.*', f)]

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

    # LEXICON
    lex_filename = '/Users/Shared/data/onlinemedia/repository/lexicon_march.csv'
    lexicon_list = list()
    with open(lex_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        headers = next(rows, None)
        for r in rows:
            #r[3] = r[3].replace('*', '')  # only needed when lexicon in construction
            lexicon_list.append(r)
    attributes = {"table": {"name": lex_filename, "version": 1, "columns": list()}}
    for h in headers:
        attributes["table"]["columns"].append({"name": h})
    lex_msg = api.Message(attributes=attributes, body=lexicon_list)
    setup_lexicon(lex_msg)


    # saving outcome as word index
    word_type_counter = {t: collections.Counter() for t in supported_word_types}
    for m in api.queue:
        for article in m.body:
            wdict = {w: c for w, c in article[3]}
            word_type_counter[article[2]].update(collections.Counter(wdict))

    with open('/Users/Shared/data/onlinemedia/repository/word_extraction.csv', 'w') as f:
        writer = csv.writer(f)
        cols = ['type', 'word', 'count']
        writer.writerow(cols)
        for t in word_type_counter:
            mc = word_type_counter[t].most_common()
            for w, c in mc:
                writer.writerow([t, w, c])

    with open('/Users/Shared/data/onlinemedia/repository/word_extraction.pickle', 'wb') as f:
        pickle.dump(api.queue, f)


if __name__ == '__main__':
    test_operator()

    if False :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.1',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])




