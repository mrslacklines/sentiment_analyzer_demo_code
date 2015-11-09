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
TWITTER_LISTENER = None
CONFIG = None
CITIES = None
DB = MongoClient(host='mongo')['mbsaa']
COLLECTIONS = {
    'TWITTER': DB['twitter'],
    'TRIPADVISOR': DB['tripadvisor'],
    'SKYSCRAPER': DB['skyscraper'],
}


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


@app.route('/twitter/start_listening/')
def twitter_start_listening():
    TWITTER_LISTENER = Popen(
        ['python', 'twitter/listen_to_twitter_stream.py', 'config.yml'],
        cwd='/opt/mbsaa/')
    sleep(10)
    poll_result = TWITTER_LISTENER.poll()
    return jsonify(**{'status': poll_result})


@app.route('/twitter/stop_listening/')
def twitter_stop_listening():
    if TWITTER_LISTENER:
        TWITTER_LISTENER.kill()
        status = 'kill signal sent'
    else:
        status = 'no active twitter stream listener found'
    return jsonify(**{'status': status})


@app.route('/stop_crawls/')
def stop_crawls():
    if PROCESS_DICT:
        for process in PROCESS_DICT.keys():
            PROCESS_DICT[process].kill()
        status = 'kill signals sent'
    else:
        status = 'no active crawls found'
    return jsonify(**{'status': status})


@app.route('/get_stats/')
@app.route('/get_stats/<city>')
def get_stats(city=None):
    if city:
        results = _get_stats(city)
        return jsonify(**results)
    else:
        results = _get_stats()
        return jsonify(**results)


def _read_config(filehandle):
    return safe_load(filehandle)


def _get_all_geo(resource):
    collection = COLLECTIONS.get(resource.upper())
    distinct_geo = [[], [], []]
    if collection:
        distinct_geo[0] = collection.distinct('geo.0')
        distinct_geo[1] = collection.distinct('geo.1')
        distinct_geo[2] = collection.distinct('geo.2')
    return distinct_geo


def _get_all_cities(resource):
    collection = COLLECTIONS.get(resource.upper())
    if collection:
        return collection.distinct('city')
    else:
        return []


def _get_stats_for_city(resource, city_name):
    collection = COLLECTIONS.get(resource.upper())
    results = {}
    if collection:
        cities, countries, continents = _get_all_geo(resource)
        for sentiment in ('positive', 'negative'):
            results[sentiment] = {}
            results[sentiment]['by_city'] = {}
            for city in cities:
                results[sentiment]['by_city'][city] = collection.find({
                    'city': city_name,
                    'sentiment': sentiment,
                    'geo.0': city}).count()
            results[sentiment]['by_country'] = {}
            for country in countries:
                results[sentiment]['by_country'][country] = collection.find({
                    'city': city_name,
                    'sentiment': sentiment,
                    'geo.1': country}).count()
            results[sentiment]['by_continent'] = {}
            for continent in continents:
                results[sentiment]['by_continent'][continent] = \
                    collection.find({
                        'city': city_name,
                        'sentiment': sentiment,
                        'geo.2': sentiment}).count()
            results[sentiment]['total'] = collection.find({
                'city': city_name,
                'sentiment': sentiment}).count()
        results['all'] = {}
        results['all']['by_city'] = {}
        for city in cities:
            results['all']['by_city'][city] = collection.find({
                'city': city_name,
                'geo.0': city}).count()
        results['all']['by_country'] = {}
        for country in countries:
            results['all']['by_country'][country] = collection.find({
                'city': city_name,
                'geo.1': country}).count()
        results['all']['by_continent'] = {}
        for continent in continents:
            results['all']['by_continent'][continent] = \
                collection.find({
                    'city': city_name,
                    'geo.2': sentiment}).count()
        results['all']['total'] = collection.find({'city': city_name}).count()
    return results


def _get_stats(city=None):
    all_results = {}
    for resource in ('TWITTER', 'TRIPADVISOR', 'SKYSCRAPER'):
        all_results[resource] = {}
        if not city:
            for city_name in _get_all_cities(resource):
                results = _get_stats_for_city(resource, city_name)
                all_results[resource][city_name] = results
        else:
            allresults[resource] = _get_stats_for_city(resource, city)
    return all_results


def main(arguments):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'config', help="Config file", type=argparse.FileType('r'))
    args = parser.parse_args(arguments)
    CONFIG = _read_config(args.config)
    app.run(host='127.0.0.1')


if __name__ == '__main__':
    main(sys.argv[1:])
