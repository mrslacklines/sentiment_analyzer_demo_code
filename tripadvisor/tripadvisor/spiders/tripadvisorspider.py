# -*- coding: utf-8 -*-

import argparse
import re
import scrapy
import sys

from datetime import datetime, timedelta
from redis import Redis
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.utils.project import get_project_settings
from yaml import safe_load


class TripadvisorItem(scrapy.Item):
    city = scrapy.Field()
    title = scrapy.Field()
    review = scrapy.Field()
    geo = scrapy.Field()
    date = scrapy.Field()

    def __init__(self, city, title, review, geo, date):
        self.city = city
        self.title = title
        self.review = review
        self.geo = geo
        self.date = date


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

    def __init__(self):
        self._rules = (
            Rule(self.category, follow=True),
            Rule(self.next_page, follow=True),
            Rule(self.city_search_result, follow=True),
            Rule(self.sidebar_categories, follow=True),
            Rule(self.properties, follow=True, callback=self.parse_properties)
        )
        self.redis = Redis(
            host='redis',
            db=configuration.get('REDIS_SETTINGS', {}).get('TRIPADVISOR_DB'))

    def _clean_post_text(self, post_text):
        post_text = re.sub('<[^>]+>', '', post_text)
        post_text = re.sub('\\n', '', post_text)
        return post_text

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

                request = scrapy.Request(
                    expand_url, callback=self.parse_reviews)
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
                city='test',
                geo=post_location,
                review=post_text,
                date=post_date,
                title=post_title
            )
            yield item


def read_config(filehandle):
        return safe_load(filehandle)


def main(arguments):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'config', help="Config file", type=argparse.FileType('r'))

    args = parser.parse_args(arguments)

    configuration = read_config(args.config)
    cities = configuration.get('TRIPADVISOR', {}).get('CITIES')
    search_url = configuration.get('TRIPADVISOR', {}).get('URL') + '/Search?q='
    start_urls = [search_url + re.sub(' ', '+', city) for city in cities]

    process = CrawlerProcess(get_project_settings())

    process.crawl(TripAdvisorSpider)
    process.start()


if __name__ == '__main__':
    main(sys.argv[1:])



    # def parse_forum(self, response):
    #     for row in response.xpath('//table/tr'):
    #         if row.xpath('./td/@class').extract_first() == 'tHead':
    #             continue
    #         topic_url = row.xpath("./td/b/a/@href").extract_first()
    #         url = response.urljoin(topic_url)
    #         request = scrapy.Request(url, callback=self.parse_forum_thread)
    #         request.meta['city_name'] = response.meta['city_name']
    #         yield request

    # def parse_forum_thread(self, response):
    #     self.parse_forum_thread_page(response)
    #     css_selector = ".pagination > #pager_top2 > .guiArw.sprite-pageNext"
    #     next_url = response.css(css_selector).extract_first()
    #     if next_url:
    #         url = response.urljoin(next_url)
    #         request = scrapy.Request(
    #             url, callback=self.parse_forum_thread)
    #         request.meta['city_name'] = response.meta['city_name']
    #         yield request


    # def parse_forum_thread_page(self, response):
    #     post_title = \
    #         response.xpath('.//h1[@id="HEADING"]/text()').extract_first()
    #     post_title = self._clean_post_text(post_title)
    #     for post in response.css('.post'):
    #         paragraphs = post.css('.postBody').extract()
    #         post_text = self._clean_post_text(' '.join(paragraphs))
    #         post_date_str = \
    #             post.css('.postDate').xpath('.//text()').extract_first()
    #         post_date = datetime.strptime(post_date_str, '%b %d, %Y, %I:%M %p')
    #         post_location = \
    #             post.css('.location').xpath('text()').extract_first()
    #         item = TripadvisorItem(
    #             city=response.meta['city_name'],
    #             geo=post_location,
    #             review=post_text,
    #             date=post_date,
    #             title=post_title
    #         )
    #         yield item
