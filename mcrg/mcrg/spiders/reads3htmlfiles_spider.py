import scrapy
import boto
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import os
import sys
import boto.s3.connection
conn = boto.connect_s3(
    aws_access_key_id='AKIAJU25YD4BJX4CQJ3A',
    aws_secret_access_key='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',
    host='s3.ca-central-1.amazonaws.com',
)
b = conn.get_bucket('devmcrgzcodehisthtml', validate=False)
from boto.s3.key import Key
import urllib.request
from mcrg.items import McrgItem
import logging
from scrapy.utils.log import configure_logging

configure_logging(install_root_handler=False)
logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO
    )

# table name array to pass item pipleline while execute the query
tables = ['By Season','By Month','By month and season','By day of week','By League','By Bet Value','By Game Winner Pick Stars Count','By Underdog Pick Stars Count','By Over/Under Pick Stars Count']

class ReadS3HtmlFilesSpider(scrapy.Spider):
    id = 2
    name = 'reads3html_spider'
    download_delay = 0
    start_urls = ['http://learnsharecorner.com/']    

    def start_requests(self):
        yield scrapy.Request('http://learnsharecorner.com/', self.parse_s3html)
          
    def parse_s3html(self, response):
        for key in b.list():
            url = "https://s3.ca-central-1.amazonaws.com/devmcrgzcodehisthtml/" + \
                str(key.name)
            #url = "https://s3.ca-central-1.amazonaws.com/devmcrgzcodehisthtml/edis_soccer_system.html"
            # self.logging.info("Just arrived at %s", url)
            logging.info('Just arrived at %s' % url)
            print("=======System html url=======")
            print(url)
            print("==============================")
            # logger.INFO("This is a info message")
            with urllib.request.urlopen(url) as url:
                s = url.read()
                item = McrgItem()
                system_name = Selector(text=s).xpath(
                    '//div[@class="post-header"]//h2[@class="title"]/text()').extract()
                item["system_name"] = system_name[0].replace("'","")
                all_tabs = Selector(text=s).xpath(
                    '//div[@class="TabsContainer"]//div[@class="Tabs"]/div[@class="Tab"]').extract()
                print("==================count=================")
                print(len(all_tabs))
                print("=========================================")
                for index, tab in enumerate(all_tabs):
                    if index > 0:  # skip the first tab which contains all trends data
                        trend_name = Selector(text=tab).xpath(
                            '//h3/text()').extract()
                        # it will fetch trend name
                        item['trend_name'] = trend_name[0].replace("'","")
                        print("===============trend name==================")
                        print(item['trend_name'])
                        print("===========================================")
                        get_all_stat = Selector(text=tab).xpath(
                            '//div[re:test(@class, "typPerformance\d+$")]//div[@class="Stat"]').extract()
                        print("==================get_all_stat=================")
                        print(len(get_all_stat))
                        print("=========================================")
                        for stat in get_all_stat:
                            table_name = Selector(text=stat).xpath('//div[@class="Stat"]/h3/text()').extract()
                            print("==================table_name=================")
                            print(table_name)
                            print("=========================================")
                            if table_name[0] in tables: 
                                rows = Selector(text=stat).xpath(
                                    '//div[@class="Stat"]//table//tr').extract()
                                for index, row in enumerate(rows):
                                    if index > 0:  # skip first row heading one, as we need data                                    
                                        cols = Selector(text=row).xpath(
                                            '//td').extract()
                                        row_number = Selector(text=cols[0]).xpath('//td/text()').extract()
                                        value = Selector(text=cols[1]).xpath('//td/text()').extract()
                                        bet_win_count = Selector(text=cols[2]).xpath('//td/text()').extract()
                                        bet_win_percentage = Selector(text=cols[3]).xpath('//td/text()').extract()
                                        bet_loss_count = Selector(text=cols[4]).xpath('//td/text()').extract()
                                        bet_loss_percentage = Selector(text=cols[5]).xpath('//td/text()').extract()
                                        bet_push_count = Selector(text=cols[6]).xpath('//td/text()').extract()
                                        bet_push_percentage = Selector(text=cols[7]).xpath('//td/text()').extract()
                                        bet_total_count = Selector(text=cols[8]).xpath('//td/text()').extract()
                                        bet_total_percentage = Selector(text=cols[9]).xpath('//td/text()').extract()
                                        profit = Selector(text=cols[10]).xpath('//td/text()').extract()
                                        print("=========Row Data============")
                                        print("==============================")
                                        # Fetching the dynamic table name and passing to item pipeline
                                        if table_name[0] == "By Season":
                                            item['table_name'] = 'zcode_system_by_season'
                                        if table_name[0] == "By Month":
                                            item['table_name'] = 'zcode_system_by_month'
                                        if table_name[0] == "By month and season":
                                            item['table_name'] = 'zcode_system_by_month_and_season'
                                        if table_name[0] == "By day of week":
                                            item['table_name'] = 'zcode_system_by_day_of_week'
                                        if table_name[0] == "By League":
                                            item['table_name'] = 'zcode_system_by_league'
                                        if table_name[0] == "By Bet Value":
                                            item['table_name'] = 'zcode_system_bet_by_value'
                                        if table_name[0] == "By Game Winner Pick Stars Count":
                                            item['table_name'] = 'zcode_system_by_game_winner_pick_stars_count'
                                        if table_name[0] == "By Underdog Pick Stars Count":
                                            item['table_name'] = 'zcode_system_by_underdog_pick_stars_count'
                                        if table_name[0] == "By Over/Under Pick Stars Count":
                                            item['table_name'] = 'zcode_system_by_overunder_pick_stars_count'

                                        if not row_number:
                                            item['row_number'] = 0
                                        else:    
                                            item['row_number'] = row_number[0]
                                        if not value:
                                            item['value'] = ''
                                        else:
                                            item['value'] = value[0]
                                        if not bet_win_count:
                                            item['bet_win_count'] = 0
                                        else:
                                            item['bet_win_count'] = bet_win_count[0]
                                        if not bet_win_percentage:
                                            item['bet_win_percentage'] = 0
                                        else:
                                            item['bet_win_percentage'] = bet_win_percentage[0]
                                        if not bet_loss_count:
                                            item['bet_loss_count'] = 0
                                        else:
                                            item['bet_loss_count'] = bet_loss_count[0]
                                        if not bet_loss_percentage:
                                            item['bet_loss_percentage'] = 0
                                        else:
                                            item['bet_loss_percentage'] = bet_loss_percentage[0]
                                        if not bet_push_count:
                                            item['bet_push_count'] = 0
                                        else:
                                            item['bet_push_count'] = bet_push_count[0]
                                        if not bet_push_percentage:
                                            item['bet_push_percentage'] = 0
                                        else:
                                            item['bet_push_percentage'] = bet_push_percentage[0]
                                        if not bet_total_count:
                                            item['bet_total_count'] = 0
                                        else:
                                            item['bet_total_count'] = bet_total_count[0]
                                        if not bet_total_percentage:
                                            item['bet_total_percentage'] = 0
                                        else:
                                            item['bet_total_percentage'] = bet_total_percentage[0]
                                        if not profit:
                                            item['profit'] = 0
                                        else:
                                            item['profit'] = profit[0]

                                        yield item      


