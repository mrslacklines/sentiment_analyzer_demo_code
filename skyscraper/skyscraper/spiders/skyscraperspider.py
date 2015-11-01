# -*- coding: utf-8 -*-

import re
import scrapy

from datetime import date, datetime, timedelta
from redis import Redis
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
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
        self.redis = Redis(
            host='redis',
            db=self.configuration.get(
                'REDIS_SETTINGS', {}).get('SKYSCRAPER_DB'))
        cities = self.configuration.get('SKYSCRAPER', {}).get('CITIES')
        start_urls = [self.configuration.get('SKYSCRAPER', {}).get('URL'),]
        self._rules = (
            Rule(self.forum, follow=True),
            Rule(self.next_page, follow=True),
            Rule(self.thread, follow=True, callback=self.parse_posts)
        )

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
        for post in response.xpath('//table[contains(@id, "post")]'):
            city = 'test'
            post_xpath = './/div[contains(@id, "post_message_")]/text()'
            post_text = self._clean_post_text(
                " ".join(post.xpath(post_xpath).extract()))
            location_xpath = ('.//td[contains(concat(" ", normalize-space('
                              '@class), " "), " alt2 ")]/table/tr/td['
                              'contains(concat(" ", normalize-space(@valign'
                              '), " ")," top ")]/div/div[2]/text()')
            geo = post.xpath(location_xpath).re('Location: (.+)')
            date_xpath = ('.//td[contains(concat(" ", normalize-space('
                          '@class), " "), " thead ")]/div/span/span/b/text()')
            date = self._extract_time(post.xpath(date_xpath).extract_first())
            # self.redis.lpush(
            #     city.lower(), {
            #         'ts': status.timestamp_ms,
            #         'text': status.text,
            #         'location': status.place})

        pass
