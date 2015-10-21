# -*- coding: utf-8 -*-

import re
import scrapy


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
        import ipdb; ipdb.set_trace()  # breakpoint fe0c7feb //
        pass
