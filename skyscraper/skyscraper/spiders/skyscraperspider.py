# -*- coding: utf-8 -*-

import pickle
import re
import rethinkdb
import scrapy

from datetime import date, datetime, timedelta
from incf.countryutils.datatypes import Country
from nltk import bigrams
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from pymongo import MongoClient
from yaml import safe_load

from skyscraper.items import SkyScraperItem


class SkyScraperSpider(CrawlSpider):
    name = "ss_spider"
    allowed_domains = ["forum.skyscraperpage.com"]
    forum_link_xpath = ('.//tr/td[2][contains(concat(" ", normalize-space('
                        '@class), " "), " alt1")]')
    forum = LinkExtractor(restrict_xpaths=forum_link_xpath)
    thread = LinkExtractor(restrict_xpaths='//tr/td[3]/div')
    next_page = LinkExtractor(restrict_xpaths='//a[@rel="next"]')

    def __init__(self, config):
        self.configuration = self._read_config(config)
        self.mongo = MongoClient(
            self.configuration.get('MONGO', {}).get('HOST'),
            int(self.configuration.get('MONGO', {}).get('PORT'))
        )
        self.db_name = self.configuration.get('MONGO', {}).get('DB_NAME')
        self.collection = self.configuration.get('MONGO', {}).get(
            'SKYSCRAPER_COLLECTION')
        self.db = self.mongo[self.db_name][self.collection]
        cities = self.configuration.get('SKYSCRAPER', {}).get('CITIES')
        rethinkdb.connect('rethinkdb', '28015').repl()
        rethinkdb.connect('rethinkdb', '28015').repl()
        self.gazetteer = rethinkdb.db('geonames').table('geonames')
        self.classifier_file = self.configuration.get('CLASSIFIER', {}).get(
            'MODELS_DIR') + '/reviews'
        self.classifier = pickle.load(open(self.classifier_file, 'r'))
        self.cities = self.configuration.get('SKYSCRAPER', {}).get('CITIES')
        self.base_url = self.configuration.get('SKYSCRAPER', {}).get('URL')
        self.start_urls = [self.base_url,]
        self._rules = (
            Rule(self.forum, follow=True),
            Rule(self.next_page, follow=True),
            Rule(self.thread, follow=True, callback=self.parse_posts)
        )

    def _word_feats(self, words):
        return dict([(word, True) for word
                     in list(words) + list(bigrams(words))])

    def _classify(self, string):
        return self.classifier.classify(self._word_feats(string.split()))

    def _read_config(self, filename):
        filehandle = open(filename, 'r')
        return safe_load(filehandle)

    def _extract_time(self, string):
        if 'Yesterday' in string:
            hour = re.match('Yesterday, (\d{1,2}:\d{2} [AP]M)', string)
            if hour:
                yesterday = date.today() - timedelta(1)
                time = datetime.strptime(hour.groups()[0], '%I:%M %p')
                return datetime.combine(yesterday, time.time())
            else:
                return
        elif 'Today' in string:
            hour = re.match('Today, (\d{1,2}:\d{2} [AP]M)', string)
            if hour:
                today = date.today()
                time = datetime.strptime(hour.groups()[0], '%I:%M %p')
                return datetime.combine(today, time.time())
            else:
                return
        return datetime.strptime(string, '%b %d, %Y, %I:%M %p')

    def _clean_post_text(self, post_text):
        post_text = re.sub(r'<[^>]+>', r'', post_text)
        post_text = re.sub(r'[\n\r\t]', r' ', post_text)
        post_text = re.sub(r' +', r' ', post_text)
        return post_text

    def parse_posts(self, response):
        thread_topic_xpath = ('//td[contains(@class, "alt1")]/table/tr/'
                              'td[contains(@class, "navbar")]/strong/text()')
        thread_topic = re.sub(
            r'[\n\r\t]',
            r' ',
            response.xpath(thread_topic_xpath).extract_first()).lower().strip()
        for city_name in self.cities:
            if re.match(r'^' + city_name.lower(), thread_topic):
                city = city_name
                for post in response.xpath('//table[contains(@id, "post")]'):
                    post_xpath =\
                        './/div[contains(@id, "post_message_")]/text()'
                    post_text = self._clean_post_text(
                        " ".join(post.xpath(post_xpath).extract()))
                    location_xpath = \
                        ('.//td[contains(concat(" ", normalize-space('
                         '@class), " "), " alt2 ")]/table/tr/td['
                         'contains(concat(" ", normalize-space(@valign'
                         '), " ")," top ")]/div/div[2]/text()')
                    geo = post.xpath(location_xpath).re('Location: (.+)')
                    if geo:
                        geo = geo[0]
                    else:
                        geo = None
                    date_xpath = ('.//td[contains(concat(" ", normalize-space('
                                  '@class), " "), " thead ")]'
                                  '/div/span/span/b/text()')
                    date = self._extract_time(
                        post.xpath(date_xpath).extract_first())
                    item = SkyScraperItem(
                        city=city_name, geo=geo, text=post_text, date=date)
                    self.process_and_add_to_db(item)
                    yield item

    def process_and_add_to_db(self, item):
        country = None
        continent = None
        if item['geo']:
            city_name = re.sub(r'^([\w\s]+).+$', '\g<1>', item['geo'])
            cities = self.gazetteer.filter(
                lambda location: location['name'].downcase().match(
                    city_name)).run()
            city_results = sorted(
                [city for city in cities],
                key=lambda k: int(k.get('population')))
            if city_results:
                city = city_results[-1]
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
