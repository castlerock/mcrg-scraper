import scrapy
from scrapy.selector import Selector
from mcrg.items import CoversLineHistoryItem
import logging
from scrapy.utils.log import configure_logging
from datetime import datetime, timedelta

configure_logging(install_root_handler=False)
logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO)

# Only need from 2011 - 2016, 2010 don't have much so leave it.  
base_url = "http://www.covers.com/sports/MLB/matchups?selectedDate="      
seasons = ['2017','2016','2015','2014','2013','2012','2011']
start_dates = ['2017-02-24','2016-03-01','2015-03-03','2014-02-26','2013-02-22','2012-03-02','2011-02-25']
end_dates = ['2017-03-22','2016-11-02','2015-11-01','2014-10-29','2013-10-30','2012-10-28','2011-10-28']

class CoversSpider(scrapy.Spider):
    id = 3
    name = 'covers_mlb_line_history'
    download_delay = 0
    start_urls = ['http://www.covers.com/']

    def start_requests(self):
        yield scrapy.Request('http://www.covers.com/sports/mlb/matchups', self.parse)
    
    def parse(self, response):
        logging.info('Just arrived at %s' % response.url)               
        # seasons = response.xpath("//div[@class='cmg_navigation_conference_season']//span//select[@id='cmg_season_dropdown']/option/text()").extract()
        # all_urls = []
        for index, season in enumerate(seasons):
            start_url = base_url + end_dates[index]  
            end_url = base_url + start_dates[index]
            logging.info('Just arrived at %s' % start_url)
            count = 0            
            while start_url != end_url:
                dd =  datetime.strptime(end_dates[index], '%Y-%m-%d').date()
                new_date = dd - timedelta(days=count)
                count = count + 1
                start_url = base_url + new_date.strftime("%Y-%m-%d")
                print("====start url %s" % start_url)
                yield scrapy.Request(start_url, self.parse_line_history_url, meta={'season': season})   
                # all_urls.append(start_url)
        # for u in all_urls:
        #     print("start url %s" % u)
        #     yield scrapy.Request(u, self.parse_line_history_url)    
    
    # Parsing line history page urls
    def parse_line_history_url(self, response):
        season = response.meta.get('season')
        print("at parse_line_history_url season and url ", season, response.url)
        logging.info('Just arrived at parse_line_history_url and url %s' % response.url)
        urls = response.xpath("//div[@class='cmg_matchups_list']//div[@class='cmg_l_row cmg_matchup_list_gamebox']/a[text()='Line History']/@href").extract()
        for url in urls:
            yield scrapy.Request(url, self.parse_line_history, meta={'season': season})

    # Parsing html of line history page
    def parse_line_history(self, response):
        season = response.meta.get('season')
        print("at parse_line_history ", response.url)
        logging.info('Just arrived at parse_line_history and url %s' % response.url)
        item = CoversLineHistoryItem()
        item['season'] = season
        team = response.xpath("//div[@class='leftCol']//h3/text()").extract()
        item['team_name'] = team[0].replace('\r\n','')
        match_date_time = response.xpath("//div[@class='leftCol']//h3/div[@class='right']/text()").extract()
        item['match_date_time'] = match_date_time[0]
        reference_website_url_0 = ""
        reference_website_name_0 = ""
        trs = response.xpath("//table[@id='bodyContentPlaceHolder_ucLineHistory_tblLineHistory']//tr").extract()
        for tr in trs:
            td = Selector(text=tr).xpath('//td').extract()
            reference_website_url = Selector(text=td[0]).xpath("//td/a/@href").extract()
            reference_website_name = Selector(text=td[0]).xpath("//td/a/text()").extract()
            if not reference_website_url:
                time = Selector(text=td[0]).xpath('//td/text()').extract()
                item['time'] = time[0]
                line = Selector(text=td[1]).xpath('//td/text()').extract()
                item['line'] = line[0]
                over_under = Selector(text=td[2]).xpath('//td/text()').extract()
                item['over_under'] = over_under[0]
                item['reference_website_url'] = reference_website_url_0
                item['reference_website_name'] = reference_website_name_0
                print("==========================")
                print(item)
                print("==========================")
                yield item
            else:
                reference_website_url_0 = reference_website_url[0]
                reference_website_name_0 = reference_website_name[0]
                item['reference_website_url'] = reference_website_url_0
                item['reference_website_name'] = reference_website_name_0
                # print(reference_website_url_0, reference_website_name_0)
            

        

    

