
import logging
import re

from scrapy.http import Response

logger = logging.getLogger(__name__)


class CityMiddleware(object):

    """This middleware adds city information to response.meta"""

    def process_request(self, request, spider):
        city_name = re.match(
            spider.base_url + '/Search\?q\=(\w+)', request.url)
        if city_name and not hasattr(request.meta, 'city'):
            request.meta['city'] = re.sub('\+', ' ', city_name.groups()[0])
