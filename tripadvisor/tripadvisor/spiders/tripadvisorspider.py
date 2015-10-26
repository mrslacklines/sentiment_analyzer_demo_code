# -*- coding: utf-8 -*-

import re
import scrapy

from datetime import datetime, timedelta
from redis import Redis

from tripadvisor.items import TripadvisorItem


cities = ['new york city']


class TripAdvisorSpider(scrapy.Spider):
    name = "ta_spider"
    allowed_domains = ["tripadvisor.com"]
    start_urls = [
        "http://www.tripadvisor.com",
    ]

    def __init__(self):
        self.redis = Redis(host='redis')

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
                if "-c2-" in link:
                    continue
                url = response.urljoin(link)
                request = scrapy.Request(url, callback=self.parse_review_type)
                request.meta['city_name'] = response.meta['city_name']
                yield request

    def parse_review_type(self, response):
            review_type = response.xpath(
                './/*[@id="MAIN"]/@class').extract_first()
            heading = response.xpath(
                './/*[@id="HEADING"]/text()').extract_first()
            if "Hotels" in review_type and "Bed and Breakfast" in heading:
                pass
            elif "Hotels" in review_type:
                for result in self.parse_hotels_pagination(response):
                    yield result
            elif "Restaurants" in review_type:
                self.parse_restaurants(response)
            elif "Travel_Guide" in review_type:
                pass
            elif "Attractions" in review_type:
                pass
            elif "VacationRentals" in review_type:
                pass

    def parse_hotels_pagination(self, response):
        for hotel in self.parse_hotels(response):
            yield hotel
        next_url = response.css(
            '.pagination > .nav.next').xpath('./@href').extract_first()
        if next_url:
            url = response.urljoin(next_url)
            request = scrapy.Request(
                url, callback=self.parse_hotels_pagination)
            request.meta['city_name'] = response.meta['city_name']
            yield request

    def parse_hotels(self, response):
        hotel_urls = \
            response.css('.listing_title').xpath('./a/@href').extract()
        for hotel_url in hotel_urls:
            url = response.urljoin(hotel_url)
            request = scrapy.Request(
                url, callback=self.parse_hotel_review_pagination)
            request.meta['city_name'] = response.meta['city_name']
            yield request

    def parse_hotel_review_pagination(self, response):
        for review in self.expand_hotel_reviews(response):
            yield review
        # TODO
        pass

    def expand_hotel_reviews(self, response):
        review_ids = response.css(
            '.reviewSelector').xpath('./@id').re('review_([0-9]+)')
        hotel_id_re = (r'http://www\.tripadvisor\.com/Hotel_Review-'
                       '([a-z][0-9]+-[a-z][0-9]+)-.+\.html')
        hotel_id = re.match(hotel_id_re, response.url)
        if hotel_id and review_ids:
            hotel_id = hotel_id.groups()[0]
        expand_url = 'http://www.tripadvisor.com/ExpandedUserReviews-'
        expand_url += hotel_id + '?'
        expand_url += 'target=' + review_ids[0] + '&'
        expand_url += 'context=1&'
        expand_url += 'reviews=' + ','.join(review_ids) + '&'
        expand_url += 'servlet=Hotel_Review&expand=1'

        request = scrapy.Request(
            expand_url, callback=self.parse_hotel_reviews)
        request.meta['city_name'] = response.meta['city_name']
        yield request

    def parse_hotel_reviews(self, response):
        # TODO
        pass

    def parse_restaurants(self, response):
        # TODO
        pass

    def parse_city_frontpage(self, response):
        forum_url = response.xpath(
            "//a[@data-trk='forum_nav']/@href").extract_first()
        if "forum" not in forum_url.lower():
            return
        url = response.urljoin(forum_url)
        request = scrapy.Request(
            url, callback=self.process_forum_pagination)
        request.meta['city_name'] = response.meta['city_name']
        yield request

    def process_forum_pagination(self, response):
        for thread in self.parse_forum(response):
            yield thread
        next_url = response.css(".guiArw.sprite-pageNext").xpath(
            './/@href').extract_first()
        if next_url:
            url = response.urljoin(next_url)
            request = scrapy.Request(
                url, callback=self.process_forum_pagination)
            request.meta['city_name'] = response.meta['city_name']
            yield request

    def parse_forum(self, response):
        for row in response.xpath('//table/tr'):
            if row.xpath('./td/@class').extract_first() == 'tHead':
                continue
            topic_url = row.xpath("./td/b/a/@href").extract_first()
            url = response.urljoin(topic_url)
            request = scrapy.Request(url, callback=self.parse_forum_thread)
            request.meta['city_name'] = response.meta['city_name']
            yield request

    def parse_forum_thread(self, response):
        self.parse_forum_thread_page(response)
        css_selector = ".pagination > #pager_top2 > .guiArw.sprite-pageNext"
        next_url = response.css(css_selector).extract_first()
        if next_url:
            url = response.urljoin(next_url)
            request = scrapy.Request(
                url, callback=self.parse_forum_thread)
            request.meta['city_name'] = response.meta['city_name']
            yield request

    def _clean_post_text(self, post_text):
        post_text = re.sub('<[^>]+>', '', post_text)
        post_text = re.sub('\\n', '', post_text)
        return post_text

    def parse_forum_thread_page(self, response):
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
                city=response.meta['city_name'],
                geo=post_location,
                review=post_text,
                date=post_date,
                title=post_title
            )
            yield item
