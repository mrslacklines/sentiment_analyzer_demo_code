import argparse
import sys

from flask import abort, Flask
from flask.json import jsonify
from pymongo import MongoClient
from subprocess import Popen
from time import sleep
from yaml import safe_load

DEBUG = True

app = Flask(__name__)
app.config.from_object(__name__)

PROCESS_DICT = {}
CONFIG = None
CITIES = None
DB_TWITTER = None
DB_TRIPADVISOR = None
DB_SKYSCRAPER = None


@app.route('/start_spider/<spider_name>')
def start_spider(spider_name):
    dir = '/opt/mbsaa/' + spider_name + '/'
    if spider_name.lower() == 'tripadvisor':
        spider_name = 'ta_spider'
    elif spider_name.lower() == 'skyscraper':
        spider_name = 'ss_spider'
    else:
        return abort(404)
    PROCESS_DICT[spider_name] = Popen(
        ['scrapy', 'crawl', spider_name, '-a', 'config=/opt/mbsaa/config.yml'],
        cwd=dir)
    sleep(10)
    return jsonify(**{'status': PROCESS_DICT[spider_name].poll()})


@app.route('/resume_spider/<spider_name>')
def resume_spider(spider_name):
    dir = '/opt/mbsaa/' + spider_name + '/'
    crawls_dir_param = 'JOBDIR=crawls/' + spider_name
    if spider_name.lower() == 'tripadvisor':
        spider_name = 'ta_spider'
    elif spider_name.lower() == 'skyscraper':
        spider_name = 'ss_spider'
    else:
        return abort(404)
    PROCESS_DICT[spider_name] = Popen(
        ['scrapy', 'crawl', spider_name, '-a', 'config=/opt/mbsaa/config.yml',
         '-s', crawls_dir_param
         ],
        cwd=dir)
    sleep(10)
    return jsonify(**{'status': process.poll()})


@app.route('/stop_crawls/')
def stop_crawls():
    if PROCESS_DICT:
        for process in PROCESS_DICT.keys():
            PROCESS_DICT[process].kill()
        status = 'kill signals sent'
    else:
        status = 'no active crawls found'
    return jsonify(**{'status': status})


def _read_config(filehandle):
    return safe_load(filehandle)


@app.route('/get_stats/')
@app.route('/get_stats/<city>')
def get_stats(city=None):
    pass


def main(arguments):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'config', help="Config file", type=argparse.FileType('r'))
    args = parser.parse_args(arguments)
    CONFIG = _read_config(args.config)

    CITIES = CONFIG.get('TRIPADVISOR', {}).get('CITIES')
    app.run(host='127.0.0.1')


if __name__ == '__main__':
    main(sys.argv[1:])
