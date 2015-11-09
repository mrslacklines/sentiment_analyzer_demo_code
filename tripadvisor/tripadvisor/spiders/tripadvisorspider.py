# -*- coding: utf-8 -*-

import pickle
import re
import rethinkdb
import scrapy

from datetime import datetime, timedelta
from incf.countryutils.datatypes import Country
from nltk import bigrams
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse, Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from pymongo import MongoClient
from yaml import safe_load

from tripadvisor.items import TripadvisorItem


def process_onclick_link(value):
    extracted_link = re.sub(
        r'(http://www.tripadvisor.com)/[^/]+(\/[^.]+\.html).+', r'\1\2', value)
    return extracted_link


class TripAdvisorSpider(CrawlSpider):
    name = "ta_spider"
    allowed_domains = ["tripadvisor.com"]
    next_page = LinkExtractor(restrict_xpaths='//link[@rel="next"]',
                              tags=('link',), attrs=('href',))
    category = LinkExtractor(restrict_css='.navLinks')
    city_search_result = LinkExtractor(restrict_css='.result.GEOS',
                                       tags=('div',), attrs=('onclick',),
                                       process_value=process_onclick_link)
    sidebar_categories = LinkExtractor(restrict_css='.accommodation_type')
    properties = LinkExtractor(restrict_css='.property_title')
    thread_xpath = ('//table[contains(concat(" ", normalize-space(@class), '
                    '" "), " topics ")]/tr/td[2]/b')
    forum_thread = LinkExtractor(restrict_xpaths=thread_xpath)
    pagination_forum = LinkExtractor(restrict_css='.pgLinks')

    def __init__(self, config):
        self.configuration = self._read_config(config)
        self.mongo = MongoClient(
            self.configuration.get('MONGO', {}).get('HOST'),
            int(self.configuration.get('MONGO', {}).get('PORT'))
        )
        self.db_name = self.configuration.get('MONGO', {}).get('DB_NAME')
        self.collection = self.configuration.get('MONGO', {}).get(
            'TRIPADVISOR_COLLECTION')
        self.db = self.mongo[self.db_name][self.collection]
        cities = self.configuration.get('TRIPADVISOR', {}).get('CITIES')
        rethinkdb.connect('rethinkdb', '28015').repl()
        self.gazetteer = rethinkdb.db('geonames').table('geonames')
        self.classifier_file = self.configuration.get('CLASSIFIER', {}).get(
            'MODELS_DIR') + '/reviews'
        self.classifier = pickle.load(open(self.classifier_file, 'r'))
        self.base_url = self.configuration.get('TRIPADVISOR', {}).get('URL')
        self.start_urls = \
            [self.base_url + '/Search?q=' + re.sub(r' ', r'+', city)
             for city in cities]
        self._rules = (
            Rule(self.category, follow=True),
            Rule(self.next_page, follow=True, callback=self.parse_properties),
            Rule(self.city_search_result, follow=True),
            Rule(self.sidebar_categories, follow=True),
            Rule(self.properties, follow=True, callback=self.parse_properties),
            Rule(self.forum_thread, follow=True,
                 callback=self.parse_forum_posts),
            Rule(self.pagination_forum, follow=True,
                 callback=self.parse_forum_posts)
        )

    def _word_feats(self, words):
        return dict([(word, True) for word
                     in list(words) + list(bigrams(words))])

    def _classify(self, string):
        return self.classifier.classify(self._word_feats(string.split()))

    def _read_config(self, filename):
        filehandle = open(filename, 'r')
        return safe_load(filehandle)

    def _clean_post_text(self, post_text):
        post_text = re.sub('<[^>]+>', '', post_text)
        post_text = re.sub('\\n', '', post_text)
        return post_text

    def _requests_to_follow(self, response):
        if not isinstance(response, HtmlResponse):
            return
        seen = set()
        for n, rule in enumerate(self._rules):
            links = [lnk for lnk in rule.link_extractor.extract_links(response)
                     if lnk not in seen]
            if links and rule.process_links:
                links = rule.process_links(links)
            for link in links:
                seen.add(link)
                r = Request(url=link.url, callback=self._response_downloaded)
                r.meta.update(rule=n, link_text=link.text)
                r.meta.update(city=response.meta['city'])
                yield rule.process_request(r)

    def parse_properties(self, response):
        review_ids = response.css(
            '.reviewSelector').xpath('./@id').re('review_([0-9]+)')
        if review_ids:
            hotel_id_re = (r'http://www\.tripadvisor\.com/([A-Za-z]+_Review)-'
                           '([a-z][0-9]+-[a-z][0-9]+)-.+\.html')
            property_data = re.match(hotel_id_re, response.url)
            if property_data:
                hotel_id = property_data.groups()[1]
                expand_url = 'http://www.tripadvisor.com/ExpandedUserReviews-'

                expand_url += hotel_id + '?'
                expand_url += 'target=' + review_ids[0] + '&'
                expand_url += 'context=1&'
                expand_url += 'reviews=' + ','.join(review_ids) + '&'
                servlet = property_data.groups()[1] + property_data.groups()[0]
                expand_url += 'servlet=' + servlet + '&expand=1'

                request = Request(
                    expand_url, meta={'city': response.meta['city']},
                    callback=self.parse_reviews)
                yield request

    def parse_reviews(self, response):
        for review in response.css('div[id^=expanded_review]'):
            post_location = review.css(
                '.location').xpath('text()').extract_first()
            post_text = self._clean_post_text(' '.join(
                review.css('.entry').xpath('./p/text()').extract()))
            post_date_title = review.css(
                '.ratingDate').xpath('@title').extract_first()
            post_date_text = re.match(
                r'Reviewed ([A-Z][a-z]+ [0-9]{1,2}, [0-9]{4})',
                review.css(
                    '.ratingDate').xpath('text()').extract_first())
            if post_date_title:
                post_date = datetime.strptime(
                    post_date_title,
                    '%B %d, %Y')
            elif post_date_text:
                post_date = datetime.strptime(
                    post_date_text.groups()[0],
                    '%B %d, %Y')
            else:
                continue
            post_title = self._clean_post_text(
                review.css('.noQuotes').xpath('text()').extract_first())
            item = TripadvisorItem(
                city=response.meta['city'],
                geo=post_location,
                text=post_text,
                date=post_date
            )
            yield item

    def parse_forum_posts(self, response):
        post_title = \
            response.xpath('.//h1[@id="HEADING"]/text()').extract_first()
        post_title = self._clean_post_text(post_title)
        for post in response.css('.post'):
            paragraphs = post.css('.postBody').extract()
            post_text = self._clean_post_text(' '.join(paragraphs))
            post_date_str = \
                post.css('.postDate').xpath('.//text()').extract_first()
            post_date = datetime.strptime(post_date_str, '%b %d, %Y, %I:%M %p')
            post_location = \
                post.css('.location').xpath('text()').extract_first()
            item = TripadvisorItem(
                city=response.meta['city'],
                geo=post_location,
                text=post_text,
                date=post_date
            )
            self.process_and_add_to_db(item)
            yield item

    def process_and_add_to_db(self, item):
        country = None
        continent = None
        if item['geo']:
            city_name = re.sub(
                r'^([^\,\.\?\;\:\'\"\[\{\]\}\-\_\!\@\&|*\(\)"]+).+$',
                '\g<1>', item['geo'])
            cities = self.gazetteer.filter(
                lambda location: location['name'].downcase().match(
                    city_name)).run()
            city_results = sorted(
                [city for city in cities],
                key=lambda k: int(k.get('population')))
            if city_results:
                city_obj = city_results[-1]
                city = city_obj.get('asciname')
                country = city.get('country_code')
                continent = Country(country).continent.name
            else:
                city = item['geo']
        else:
            city = None

        self.db.insert_one({
            'city': item['city'].lower(),
            'date': item['date'],
            'text': item['text'],
            'geo': (city, country, continent),
            'sentiment': self._classify(item['text'])
        })
