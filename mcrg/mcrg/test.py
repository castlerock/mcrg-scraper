import boto
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import os, sys
import boto.s3.connection
conn = boto.connect_s3(
        aws_access_key_id = 'AKIAJU25YD4BJX4CQJ3A',
        aws_secret_access_key = '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',
        host = 's3.ca-central-1.amazonaws.com',
        )
b = conn.get_bucket('devmcrgzcodehisthtml', validate=False) 
from boto.s3.key import Key
import urllib.request

table_names = ['By Season','By Month','By month and season','By day of week','By League','By Bet Value','By Spread Value']

for key in b.list():
    url = "https://s3.ca-central-1.amazonaws.com/devmcrgzcodehisthtml/" + str(key.name)
    with urllib.request.urlopen(url) as url:
        s = url.read()
        system_name = Selector(text=s).xpath('//div[@class="post-header"]//h2[@class="title"]/text()').extract()        
        #all_trends = Selector(text=s).xpath('//div[@class="TabsContainer"]//div[@class="buttons toolbar"]/span[@class="button"]/text()').extract()
        #print(all_trends)
        all_tabs = Selector(text=s).xpath('//div[@class="TabsContainer"]//div[@class="Tabs"]/div[@class="Tab"]').extract()
        if len(all_tabs) > 0:
            print("total tabs found", len(all_tabs))
        else:
            print("no tabs found") 
        for index, tab in enumerate(all_tabs):
            if index  > 0: #skip the first tab which contains all trends data
                trend_name = Selector(text=tab).xpath('//h3/text()').extract()
                print("trend", trend_name[0])
                get_all_stat = Selector(text=tab).xpath('//div[re:test(@class, "typPerformance\d$")]//div[@class="Stat"]').extract()
                for stat in get_all_stat:
                    table_name = Selector(text=stat).xpath('//div[@class="Stat"]/h3/text()').extract()
                    # print(table_name[0])
                    if table_name[0] in table_names: # get only bet by value table not others yet
                        # print("bet by value", stat)
                        print(table_name)
                        rows = Selector(text=stat).xpath('//div[@class="Stat"]//table//tr').extract()
                        for index, row in enumerate(rows):
                            if index > 0: # skip first row heading one, as we need data
                                # print(row)
                                cols = Selector(text=row).xpath('//td').extract()
                                # print(cols)
                                # col1 = Selector(text=cols[0]).xpath('//text()').extract()
                                # print(col1[0])
                                if cols[0] == "Total":
                                    print("here less index")
                                    print(cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8], cols[9])
                                else:
                                    print(cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8], cols[9], cols[10])
  