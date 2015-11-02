
import logging
import re

from scrapy.http import Response

logger = logging.getLogger(__name__)


class CityMiddleware(object):

    """This middleware add city information to response.meta"""

    # def process_response(self, request, response, spider):
    #     if not hasattr(response, 'meta'):
    #         import ipdb
    # ipdb.set_trace()  # breakpoint c20c7f55 //

    #     response.meta['city'] = request.meta['city']
    #     import ipdb
    # ipdb.set_trace()  # breakpoint 60d978b7 //
    #     return response

    def process_request(self, request, spider):
        city_name = re.match(
            spider.base_url + '/Search\?q\=(\w+)', request.url)
        if city_name and not hasattr(request.meta, 'city'):
            request.meta['city'] = re.sub('\+', ' ', city_name.groups()[0])
