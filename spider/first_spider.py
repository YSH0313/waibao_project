# -*- coding: utf-8 -*-
import os
import re
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)).split('spider')[0])
from config.all_config import *
from string import Template


class first_spider(Manager):
    name = 'first_spider'
    custom_settings = {
        'IS_PROXY': False,
        'UA_PROXY': False,
        'X_MAX_PRIORITY': 15,
    }

    def __init__(self):
        Manager.__init__(self)
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'
        }

    def start_requests(self):
        url = 'https://www.baidu.com'
        yield MyRequests(url=url, headers=self.header, callback=self.parse, meta={'url': url}, level=1)

    def parse(self, response):
        print(response.text)


if __name__ == '__main__':
    start_run = first_spider()
    start_run.run()
