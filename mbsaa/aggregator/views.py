from __future__ import unicode_literals

from flatdict import FlatDict
from tweepy import (
    API, OAuthHandler, Stream, StreamListener, TweepError)

from mbsaa import settings


class Aggregator(StreamListener):

    counter = 0
    api = None

    def __init__(self):
        self.counter = 0
        self.api = self.api or self._set_twitter_rest_api()

    def on_error(self, status_code):
        print "status_code"

    def on_disconnect(self):
        self.counter = 0

    def on_status(self, status):
        self.counter += 1
        print str(self.counter) + ' ' + status.text

    def _set_oauth(self):
        """
        Sets Twitter OAuth for authenticated API calls
        """
        auth = OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
        auth.set_access_token(
            settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)
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

    def _get_twitter_posts_by_rest(self, keyword, since=None, limit=125):
        """
        Gets Twitter posts for a given keyword
        """
        tweets = []
        max_id = -1L
        count = 0
        filter_params = ['#' + hashtag + ' ' + '#' + keyword for hashtag
                         in settings.HASHTAGS]
        query = " OR ".join(filter_params)

        while count < settings.MAX_TWEETS:
            try:
                if (max_id <= 0):
                    new_tweets = self.api.search(
                        q=query, count=settings.MAX_QUERY, lang="en",
                        since_id=since)
                else:
                    new_tweets = self.api.search(
                        q=query, count=settings.MAX_QUERY, lang="en",
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

    def _get_twitter_posts_by_stream(self, stream, keyword):
        """
        TODO
        """
        stream = self._set_twitter_stream_api()
        filter_params = ['#' + hashtag + ', ' + '' + keyword for hashtag
                         in settings.HASHTAGS]
        stream.filter(
            track=filter_params, async=True, languages=settings.LANGUAGES)
        return stream
