# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TripadvisorItem(scrapy.Item):
    city = scrapy.Field()
    text = scrapy.Field()
    geo = scrapy.Field()
    date = scrapy.Field()
