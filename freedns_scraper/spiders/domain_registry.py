# -*- coding: utf-8 -*-

from datetime import datetime

import scrapy
from scrapy import Spider, Request, Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose


class RegistryItem(Item):
    name = 'data'  # used as db table name by pipeline
    # TODO: sqlite_keys
    # see https://github.com/RockyZ/Scrapy-sqlite-item-exporter/blob/master/exporters.py
    domain = Field()
    host_count = Field()
    website = Field()
    status = Field()
    owner = Field()
    created_on = Field()


class RegistryItemLoader(ItemLoader):
    default_item_class = RegistryItem
    default_output_processor = TakeFirst()
    # host_count_in = MapCompose(int)
    # created_on_in = MapCompose(
    #     lambda s: datetime.strptime(s, '%m/%d/%Y').date())


class RegistrySpider(Spider):
    name = 'domain-registry'
    allowed_domains = ['freedns.afraid.org']
    base_url = 'http://freedns.afraid.org/domain/registry/'
    start_urls = [base_url]
    requests_made = True

    def parse(self, response):
        """
        @url http://freedns.afraid.org/domain/registry/
        @returns items 1 100
        @scrapes domain host_count website status owner created_on
        """
        table = response.xpath('//table[form[@action="/domain/registry/"]]')
        for row in table.xpath('./tr[@class="trl" or @class="trd"]'):
            l = RegistryItemLoader(selector=row)
            l.add_xpath('domain', './td[1]/a/text()')
            l.add_xpath('host_count', './td[1]/span/text()', re=r'\d+')
            l.add_xpath('website', './td[1]/span/a/@href')
            l.add_xpath('status', './td[2]/text()')
            l.add_xpath('owner', './td[3]/a/text()')
            l.add_xpath('created_on', './td[4]/text()', re=r'\(([^)]+)\)')
            yield l.load_item()

        if not self.requests_made:
            self.requests_made = True
            last_page = int(
                table.xpath(
                    './tr[last()]//font[./text()[starts-with(., "Page ")]]//text()[contains(., " of ")]'
                ).re_first(r'\d+'))
            for page in range(2, last_page + 1):
                yield Request(self.base_url + '?page=%d' % page)
