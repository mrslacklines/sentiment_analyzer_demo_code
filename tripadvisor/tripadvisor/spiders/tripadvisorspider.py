# -*- coding: utf-8 -*-

import re
import scrapy

from datetime import datetime, timedelta

from tripadvisor.items import TripadvisorItem


cities = ['new york city', 'chicago', 'boston']


class TripAdvisorSpider(scrapy.Spider):
    name = "ta_spider"
    allowed_domains = ["tripadvisor.com"]
    start_urls = [
        "http://www.tripadvisor.com",
    ]

    def parse(self, response):
        for city_name in cities:
            request = scrapy.FormRequest.from_response(
                response, formname='PTPT_DESTINATION_FORM',
                formdata={'q': city_name},
                clickdata={'id': 'SUBMIT_DESTINATIONS'},
                callback=self.parse_search_results)
            request.meta['city_name'] = city_name
            yield request

    def parse_search_results(self, response):
        location_results = response.css('.result.GEOS')
        city_frontpage_url = re.findall(
            r'([^/]+\/[^.]+\.html)',
            location_results.xpath('.//@onclick').extract_first())
        if city_frontpage_url:
            url = response.urljoin(city_frontpage_url[0])
            request = scrapy.Request(
                url, callback=self.parse_city_frontpage)
            request.meta['city_name'] = response.meta['city_name']
            yield request
        for result in location_results:
            if response.meta['city_name'] not in result.extract().lower():
                continue
            links = result.xpath('.//a/@href').extract()
            for link in links:
                url = response.urljoin(link)
                request = scrapy.Request(url, callback=self.parse_review_type)
                request.meta['city_name'] = response.meta['city_name']
                yield request

    def parse_review_type(self, response):
        pass

    def parse_city_frontpage(self, response):
        forum_url = response.xpath(
            "//a[@data-trk='forum_nav']/@href").extract_first()
        if "forum" not in forum_url.lower():
            return
        url = response.urljoin(forum_url)
        request = scrapy.Request(url, callback=self.parse_forum)
        request.meta['city_name'] = response.meta['city_name']
        yield request

    def parse_forum(self, response):
        for row in response.xpath('//table/tr'):
            if row.xpath('./td/@class').extract_first() == 'tHead':
                continue
            date_col = row.xpath('./td').css('.datecol')
            topic_url = date_col.xpath('./a/@href').extract_first()
            url = response.urljoin(topic_url)
            request = scrapy.Request(url, callback=self.parse_forum_thread)
            request.meta['city_name'] = response.meta['city_name']
            yield request
            # date_str = date_col.xpath('./text()').extract_first()
            # if not date_str:
            #     continue
            # if re.match(r'\d{1,2}:\d{1,2}(\s[ap]m)?', date_str):
            #     date = datetime.today()
            # elif date_str == 'yesterday':
            #     date = datetime.today - timedelta(1)
            # else:
            #     date = datetime.strptime(date_str, '%b %d, %Y')

    def parse_forum_thread(self, response):
        pass

    def parse_forum_thread_page(self, response):
        for post in response.css('.postcontent'):
            paragraphs = post.css('.postBody').extract()
            post_text = ' '.join(paragraphs)
            post_date_str = post.css('.postDate').xpath('.//text()').extract()
            post_date = datetime.strptime(post_date_str, '%b %d, %Y')
