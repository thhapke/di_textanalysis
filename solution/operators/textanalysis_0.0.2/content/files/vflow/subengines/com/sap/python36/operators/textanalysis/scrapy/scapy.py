import subprocess
import os
import time
import json
import html

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

basic_HTMLescapes = {}

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[2]['name'] : # jstring
                print('PORT: {}'.format(port))
                print(msg.attributes)
                with open(os.path.join('/Users/Shared/data/onlinemedia/crawled_texts',msg.attributes['filename']),encoding="utf-8",mode='w') as f :
                    f.writelines(msg.body)
            elif port == outports[1]['name'] : # stderr
                print('PORT: {}'.format(port))
                with open('/Users/Shared/data/onlinemedia/crawled_texts/log.log',encoding="utf-8",mode='a') as f :
                    f.writelines(msg)
            elif port == outports[0]['name'] : # logging
                print('PORT: {}'.format(port))
                print(msg)
            else  : # data
                print('PORT: {}'.format(port))
                print('Attributes: \n{}'.format(msg.attributes))
                print('Body: \n{}'.format(msg.body))

        def call(config, msg1, msg2, msg3, msg4, msg5):
            api.config = config
            process(msg1, msg2, msg3, msg4, msg5)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'pandas': '', 'sdi_utils': ''}
            version = "0.0.2"
            operator_name = 'scrapy'
            operator_description = 'scrapy'
            operator_description_long = "Starts scrapy and sends output to data port as dictionary or as json string to\
jsonstr port. Log output send to stderr(scrapy) to log port."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            spider = 'None'
            config_params['spider'] = {'title': 'Spider', 'description': \
                'List of spiders for the command \'scrapy crawl <spider>\'. Naming convention for\
spider = [media]_spider.py', 'type': 'string'}
            scrapy_dir = 'None'
            config_params['scrapy_dir'] = {'title': 'Scrapy directory', 'description': \
                'Scrapy directory (absolute path)', 'type': 'string'}
            project_dir = 'None'
            config_params['project_dir'] = {'title': 'Project Directory', 'description': \
                'Scrapy project directory on container', 'type': 'string'}


###
# Format output from captured scrape stdout
###
def format_check_output(line, logger, json_elements = ['website', 'url', 'date', 'title', 'index', 'text'] ) :
    if line == '':
        return None
    try:
        adict = json.loads(line)
    except json.JSONDecodeError  as e:
        logger.error('JSON Decoding Error: {}  ({})'.format(line, e))
        return None
    # check if all elements exist
    if all(elem in adict.keys() for elem in json_elements):
        adict['text'] = adict['text'].replace('\n', ' ')
        adict['hash_text'] = hash(adict['text'])
        adict['hash_title'] = hash(adict['title'])
        return  adict
    else :
        logger.error('Not all required json elements in article record: {}'.format(line))
        return None


###
# process
###
def process(msg1, msg2, msg3, msg4, msg5):
    adict = msg1.attributes
    msg_list = [msg1, msg2, msg3, msg4, msg5]

    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging('scrapy', loglevel='DEBUG')
    else:
        logger, log_stream = slog.set_logging('scrapy', loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()
    # logger, log_stream = slog.set_logging('scrapy',loglevel=api.config.debug_mode)

    scrapy_dir = tfp.read_value(api.config.scrapy_dir)
    if not scrapy_dir:
        logger.error('Scrapy direcory mandatory entry field')
        raise ValueError('Missing Scrapy Directory')
    logger.info('Change directory to: {}'.format(scrapy_dir))
    os.chdir(scrapy_dir)

    project_dir = tfp.read_value(api.config.project_dir)
    if not project_dir:
        logger.error('Scrapy project direcory mandatory entry field')
        raise ValueError('Missing Scrapy Project Directory')

    project_dir = os.path.join(scrapy_dir,project_dir)

    new_file_list = []
    for msg in msg_list:
        filename = os.path.basename(msg.attributes["file"]["path"])

        if filename == 'spider.py':
            filename = os.path.join(project_dir, 'spiders', filename)
        else:
            filename = os.path.join(project_dir, filename)
        # copy files to directories
        try:
            with open(filename, 'wb') as fout:
                logger.info('Write to filename (binary): {}'.format(filename))
                fout.write(msg.body)
                fout.close()
        except IOError:
            logger.warning('File not found: {}'.format(filename))
            logger.debug('Current directory: {}'.format(os.getcwd()))
            f = []
            for (dirpath, dirnames, filenames) in os.walk('/home/onlinemedia/'):
                f.extend(filenames)
                break
            logger.debug('Files under directory onlinemedia: {}'.format(f))
            api.send(outports[0]['name'], log_stream.getvalue())
            time.sleep(5)
            exit(-1)
        new_file_list.append(filename)

    for f in new_file_list:
        if os.path.isfile(filename):
            logger.info('File successfully written: {} ({})'.format(filename, time.ctime(os.path.getmtime(filename))))
        else:
            logger.error('File does not exist: {}'.format(filename))

    api.send(outports[0]['name'], log_stream.getvalue())
    log_stream.seek(0)
    log_stream.truncate(0)

    spiderlist = tfp.read_list(api.config.spider)
    num_spiders = len(spiderlist)
    num_batches = 0
    num_all_articles = 0
    if api.config.start_cmd:
        for i, spider in enumerate(spiderlist) :
            media = spider.split('_')[0]
            cmd = ['scrapy', 'crawl', spider]
            logger.info('Start scrapy: {} ({}/{})'.format(cmd,i,num_spiders))
            api.send(outports[0]['name'], log_stream.getvalue())
            log_stream.seek(0)
            log_stream.truncate(0)
            #proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd = scrapy_dir,universal_newlines=True)
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=scrapy_dir,universal_newlines=True)
            #proc = subprocess.Popen(['python','/Users/Shared/data/onlinemedia/outputgen.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            #print('CWD: {}'.format(os.getcwd()))
            # api.send(outports[1]['name'], proc.stderr)

            count_articles = 0
            articles_list = list()
            # run through stdout after scrape has ended and add to batch_output
            last_article = dict()
            for line in  proc.stdout.splitlines():
                adict = format_check_output(line, logger)
                if adict:
                    adict['media'] = media
                    articles_list.append(adict)
                    last_article = adict
                    count_articles += 1

            # send result to outport
            if len(articles_list) == 0 :
                logger.warning('No articles found: {}'.format(media))
                continue

            num_batches += 1
            attributes = { k:v for k,v in last_article.items() if k in ['website','date','columns']}
            attributes['filename'] = '{}_{}.json'.format(media,last_article['date'])
            attributes['file'] = {'path' : os.path.join('/shared/onlinemedia_texts', attributes['filename'])}
            attributes['batch'] = {'index': i, 'number': num_spiders, 'last' : False}
            attributes['batch.index'] = i
            attributes['batch.number'] = num_spiders
            if i+1 == num_spiders :
                attributes['batch.last'] = True
                attributes['batch']['last'] = True
            msg = api.Message(attributes=attributes, body=articles_list)
            api.send(outports[3]['name'], msg)

            attributes['format'] = 'JSON String'
            if i+1 == len(spiderlist) :
                attributes['message.lastBatch'] = True
            msg = api.Message(attributes=attributes, body=json.dumps(articles_list,ensure_ascii=False,indent=4))
            api.send(outports[2]['name'], msg)

        logger.info('Spider completed: {} - #articles: {}'.format(spider,count_articles))
        num_all_articles += count_articles

    logger.info('Process ended: {}  '.format(time_monitor.elapsed_time()))
    logger.info('<SCAN ENDED><{}>'.format(num_batches))

    api.send(outports[0]['name'], log_stream.getvalue())
    return 0

inports = [{'name': 'spider', 'type': 'message.file', "description": "spider.py file"}, \
           {'name': 'pipelines', 'type': 'message.file', "description": "pipelines.py file"}, \
           {'name': 'items', 'type': 'message.file', "description": "pipelines.py file"}, \
           {'name': 'middlewares', 'type': 'message.file', "description": "middlewares.py file"}, \
           {'name': 'settings', 'type': 'message.file', "description": "settings.py file"}
           ]

outports = [{'name': 'log', 'type': 'string', "description": "logging"}, \
            {'name': 'stderr', 'type': 'string', "description": "stderr"}, \
            {'name': 'jsonstr', 'type': 'message.file', "description": "message with json string"}, \
            {"name": "data", "type": "message.dicts", "description": "data"}]

api.set_port_callback([inports[0]['name'], inports[1]['name'], inports[2]['name'], inports[3]['name'], inports[4]['name']], process)


def test_operator():
    #print('CWD: {}'.format(os.getcwd()))
    config = api.config
    config.debug_mode = True
    config.scrapy_dir = '/Users/d051079/OneDrive - SAP SE/GitHub/Docker/scrapy/onlinemedia'
    config.spider = 'Spiegel_spider,FAZ_spider, Sueddeutsche_spider, Elpais_spider, Elmundo_spider, Lefigaro_spider, Lemonde_spider'
    #config.spider = 'Elpais_spider'
    config.project_dir = 'onlinemedia'
    config.json_string_output = True
    config.start_cmd = True

    items_b = open('/Users/d051079/OneDrive - SAP SE/GitHub/Docker/scrapy/source_files/items.py', mode='rb').read()
    items_msg = api.Message(attributes={'file':{'path': 'items.py'}}, body=items_b)

    spiders_b = open('/Users/d051079/OneDrive - SAP SE/GitHub/Docker/scrapy/source_files/spider.py', mode='rb').read()
    spiders_msg = api.Message(attributes={'file':{'path': 'spider.py'}}, body=spiders_b)

    middlewares_b = open('/Users/d051079/OneDrive - SAP SE/GitHub/Docker/scrapy/source_files/middlewares.py',mode='rb').read()
    middlewares_msg = api.Message(attributes={'file':{'path': 'middlewares.py'}}, body=middlewares_b)

    pipelines_b = open('/Users/d051079/OneDrive - SAP SE/GitHub/Docker/scrapy/source_files/pipelines.py',mode='rb').read()
    pipelines_msg = api.Message(attributes={'file':{'path': 'pipelines.py'}}, body=pipelines_b)

    settings_b = open('/Users/d051079/OneDrive - SAP SE/GitHub/Docker/scrapy/source_files/settings.py',mode='rb').read()
    settings_msg = api.Message(attributes={'file':{'path': 'settings.py'}}, body=settings_b)

    api.call(config, items_msg, settings_msg, spiders_msg, middlewares_msg, pipelines_msg)


