#!/usr/bin/env python

"""
Twitter stream listener
Aggregates tweets for a given set of keywords
"""

import argparse
import re
import sys

from flatdict import FlatDict
from itertools import product
from redis import Redis
from requests import ConnectionError
from tweepy import (
    API, OAuthHandler, Stream, StreamListener, TweepError)
from yaml import safe_load


class Aggregator(StreamListener):

    api = None
    configuration = None

    def __init__(self, config_file_handle):
        self.configuration = self._read_config(config_file_handle)
        self.api = self.api or self._set_twitter_rest_api()
        self.redis = Redis(host='redis')

    def on_error(self, status_code):
        print status_code

    def on_disconnect(self):
        self.counter = 0

    def on_status(self, status):
        for word in re.sub(r'#', r'', status.text).lower().split():
            if word.lower() in self.configuration.get(
                    'TWITTER', {}). get('CITIES'):
                self.redis.lpush(
                    word.lower(), {
                        'ts': status.timestamp_ms,
                        'text': status.text,
                        'location': status.place})

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

    def _set_twitter_rest_api(self):
        """
        Sets Twitter authenticated REST API object
        """
        api = API(self._set_oauth())
        return api

    def _set_twitter_stream_api(self):
        """
        Sets Twitter authenticated Stream API object
        """
        auth = self._set_oauth()
        stream = Stream(auth, self)
        return stream

    def get_twitter_posts_by_rest(self, keyword, since=None, limit=125):
        """
        Gets Twitter posts for a given keyword
        """
        tweets = []
        max_id = -1L
        count = 0
        filter_params = ['#' + hashtag + ' ' + '#' + keyword for hashtag
                         in self.configuration.get(
                             'TWITTER', {}).get('HASHTAGS')]
        query = " OR ".join(filter_params)

        while count < self.configuration.get('TWITTER', {}).get('MAX_TWEETS'):
            try:
                if (max_id <= 0):
                    new_tweets = self.api.search(
                        q=query, count=self.configuration.get(
                            'OAUTH', {}).get('MAX_QUERY'), lang="en",
                        since_id=since)
                else:
                    new_tweets = self.api.search(
                        q=query, count=self.configuration.get(
                            'TWITTER', {}).get('MAX_QUERY'), lang="en",
                        since_id=since, max_id=str(max_id - 1))
                if not new_tweets:
                    print("No new tweets")
                    break
                tweets.extend(new_tweets)
                count += len(new_tweets)
                print("Downloaded {0} tweets".format(count))
                max_id = new_tweets[-1].id
            except TweepError as e:
                print(e)
                break
        return tweets

    def _check_search_request_limits(self, api):
        """
        Checks remaining Twitter rate limit time
        """
        limits = api.rate_limit_status()
        flat_limits = FlatDict(limits)
        timestamp = flat_limits.get('resources:search:/search/tweets:reset')
        return timestamp

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
