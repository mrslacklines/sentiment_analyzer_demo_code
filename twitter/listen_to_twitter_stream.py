#!/usr/bin/env python

"""
Twitter stream listener
Aggregates tweets for a given set of keywords
"""

import argparse
import re
import sys

from datetime import datetime
from flatdict import FlatDict
from itertools import product
import logging
from redis import Redis
from requests import ConnectionError
from tweepy import (
    API, OAuthHandler, Stream, StreamListener, TweepError)
from yaml import safe_load

from sentiment_analyzer.sentiment_analyzer import Classifier


logger = logging.getLogger(__name__)


class Aggregator(StreamListener):

    api = None
    configuration = None

    def __init__(self, config_file_handle):
        self.configuration = self._read_config(config_file_handle)
        self.api = self.api or self._set_twitter_rest_api()
        self.redis = Redis(host='redis', db=self.configuration.get(
            'REDIS_SETTINGS', {}).get('TWITTER_DB'))

    def on_error(self, status_code):
        logger.error(status_code)

    def on_disconnect(self):
        self.counter = 0

    def on_status(self, status):
        for word in re.sub(r'#', r'', status.text).lower().split():
            if word.lower() in self.configuration.get(
                    'TWITTER', {}). get('CITIES'):
                self.redis.lpush(
                    word.lower(), {
                        'date': status.timestamp_ms,
                        'text': status.text,
                        'geo': status.place})

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
