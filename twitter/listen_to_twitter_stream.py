#!/usr/bin/env python

"""
Twitter stream listener
Aggregates tweets for a given set of keywords
"""

import argparse
import json
import pickle
import re
import rethinkdb
import sys

from datetime import datetime
from flatdict import FlatDict
from incf.countryutils.datatypes import Country
from itertools import product
import logging
from nltk import bigrams
from pymongo import MongoClient
from tweepy import OAuthHandler, Stream, StreamListener
from yaml import safe_load

logger = logging.getLogger(__name__)


class Aggregator(StreamListener):

    def __init__(self, config_file_handle):
        self.configuration = self._read_config(config_file_handle)
        self.twitter_model = self.configuration.get(
            'CLASSIFIER', {}).get('MODELS_DIR') + '/twitter'
        self.mongo = MongoClient(
            self.configuration.get('MONGO', {}).get('HOST'),
            int(self.configuration.get('MONGO', {}).get('PORT'))
        )
        self.db_name = self.configuration.get('MONGO', {}).get('DB_NAME')
        self.collection = self.configuration.get('MONGO', {}).get(
            'TWITTER_COLLECTION')
        self.db = self.mongo[self.db_name][self.collection]
        rethinkdb.connect('rethinkdb', '28015').repl()
        self.gazetteer = rethinkdb.db('geonames').table('geonames')
        self.classifier = pickle.load(open(self.twitter_model, 'r'))

    def _word_feats(self, words):
        return dict([(word, True) for word
                     in list(words) + list(bigrams(words))])

    def _classify(self, string):
        return self.classifier.classify(self._word_feats(string.split()))

    def on_error(self, status_code):
        logger.error(status_code)

    def on_disconnect(self):
        self.counter = 0

    def on_data(self, data):
        status = json.loads(data)
        for word in re.sub(r'#', r'', status.get('text')).lower().split():
            if word.lower() in self.configuration.get(
                    'TWITTER', {}). get('CITIES'):
                text = status.get('text')
                date = datetime.fromtimestamp(
                    int(status.get('timestamp_ms'))/1000.0)
                geo = status.get('user').get('location')
                self.process_and_add_to_db(word, date, text, geo)

    def process_and_add_to_db(self, city, date, text, geo):
        country = None
        continent = None
        if geo:
            city_name = re.sub(
                r'^([^\,\.\?\;\:\'\"\[\{\]\}\-\_\!\@\&|*\(\)"]+).+$', '\g<1>',
                geo)
            cities = self.gazetteer.filter(
                lambda location: location['name'].downcase().match(
                    city_name)).run()
            city_results = sorted(
                [city for city in cities],
                key=lambda k: int(k.get('population')))
            if city_results:
                city_obj = city_results[-1]
                post_city = city_obj.get('asciname')
                country = city.get('country_code')
                continent = Country(country).continent.name
            else:
                post_city = geo
        else:
            post_city = None

        self.db.insert_one({
            'city': city.lower(),
            'date': date,
            'text': text,
            'geo': (post_city, country, continent),
            'sentiment': self._classify(text)
        })

    def _read_config(self, filehandle):
        return safe_load(filehandle)

    def _set_oauth(self):
        """
        Sets Twitter OAuth for authenticated API calls
        """
        auth = OAuthHandler(
            self.configuration.get('OAUTH', {}).get('CONSUMER_KEY'),
            self.configuration.get('OAUTH', {}).get('CONSUMER_SECRET'))
        auth.set_access_token(
            self.configuration.get('OAUTH', {}).get('ACCESS_TOKEN'),
            self.configuration.get('OAUTH', {}).get('ACCESS_TOKEN_SECRET'))
        return auth

    def _set_twitter_stream_api(self):
        """
        Sets Twitter authenticated Stream API object
        """
        auth = self._set_oauth()
        stream = Stream(auth, self)
        return stream

    def get_twitter_posts_by_stream(self):
        """
        Collects tweets for a given set of keywords
        """
        keyword_hashtag_pairs = product(
            self.configuration.get('TWITTER', {}).get('HASHTAGS'),
            self.configuration.get('TWITTER', {}).get('CITIES'))
        filter_params = ['#' + kh_pair[0] + ' ' + kh_pair[1]
                         for kh_pair in keyword_hashtag_pairs]
        stream = self._set_twitter_stream_api()
        stream.filter(
            track=filter_params, async=False, languages=self.configuration.get(
                'OAUTH', {}).get('LANGUAGES'))
        return stream


def main(arguments):

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'config', help="Config file", type=argparse.FileType('r'))

    args = parser.parse_args(arguments)
    aggregator = Aggregator(args.config)
    stream = aggregator.get_twitter_posts_by_stream()


if __name__ == '__main__':
    main(sys.argv[1:])
