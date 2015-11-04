
import logging
import re

from scrapy.exceptions import IgnoreRequest
from scrapy.http import Response

logger = logging.getLogger(__name__)


class CleanUrl(object):
    seen_urls = set()

    def process_request(self, request, spider):
        url = request.url
        if url in self.seen_urls:
            raise IgnoreRequest()
        else:
            self.seen_urls.add(url)
