#!/usr/bin/env python

"""
Twitter stream listener
Aggregates tweets for a given set of keywords
"""

import argparse
import json
import re
import sys

from datetime import datetime
from flatdict import FlatDict
from geopy import geocoders
from incf.countryutils import transformations
from incf.countryutils.datatypes import Country
from itertools import product
import logging
from redis import Redis
from tweepy import OAuthHandler, Stream, StreamListener
from yaml import safe_load

# from sentiment_analyzer.sentiment_analyzer import Classifier


logger = logging.getLogger(__name__)


class Aggregator(StreamListener):

    def __init__(self, config_file_handle):
        self.configuration = self._read_config(config_file_handle)
        self.redis = Redis(host='redis', db=self.configuration.get(
            'REDIS_SETTINGS', {}).get('TWITTER_DB'))
        self.twitter_model = self.configuration.get(
            'CLASSIFIER', {}).get('MODELS_DIR') + '/twitter'
        self.redis = Redis(
            host='redis',
            db=self.configuration.get(
                'REDIS_SETTINGS', {}).get('TWITTER_DB'))
        self.geo = geocoders.GoogleV3()

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
                place, (lat, lng) = self.geo.geocode(geo)
                geo_data = place.split(', ')
                if len(geo_data) > 1:
                    country = Country(geo_data[-1])
                    continent = country.continent
                else:
                    country = geo
                    continent = None
                self.redis.lpush(
                    word.lower(), {
                        'date': date,
                        'text': text,
                        'geo': (country, continent)})

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
