# -*- coding: utf-8 -*-

import logging

BOT_NAME = 'skyscraper'

SPIDER_MODULES = ['skyscraper.spiders']
NEWSPIDER_MODULE = 'skyscraper.spiders'

DUPEFILTER_CLASS = 'scrapy.dupefilters.BaseDupeFilter'
DOWNLOADER_MIDDLEWARES = {
    'tripadvisor.downloadermiddlewares.singlesessiondupe.CleanUrl': 1000,
}

LOG_LEVEL = logging.INFO

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""

DUPEFILTER_DEBUG = True
