# -*- coding: utf-8 -*-

import logging

BOT_NAME = 'tripadvisor'

SPIDER_MODULES = ['tripadvisor.spiders']
NEWSPIDER_MODULE = 'tripadvisor.spiders'

LOG_LEVEL = logging.INFO

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""

DUPEFILTER_DEBUG = True

DOWNLOADER_MIDDLEWARES = {
    'tripadvisor.downloadermiddlewares.citynamemiddleware.CityMiddleware': 999,
}