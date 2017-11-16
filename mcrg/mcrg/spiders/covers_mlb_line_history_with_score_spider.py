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
# seasons = ['2017','2016','2015','2014','2013','2012','2011']
# start_dates = ['2017-02-24','2016-03-01','2015-03-03','2014-02-26','2013-02-22','2012-03-02','2011-02-25']
# end_dates = ['2017-03-22','2016-11-02','2015-11-01','2014-10-29','2013-10-30','2012-10-28','2011-10-28']

# seasons = ['2015','2014','2013','2012','2011']
# start_dates = ['2015-03-03','2014-02-26','2013-02-22','2012-03-02','2011-02-25']
# end_dates = ['2015-11-01','2014-10-29','2013-10-30','2012-10-28','2011-10-28']

#only for 2017 season
seasons = ['2017']
start_dates = ['2017-07-26']
end_dates = ['2017-08-01']

class CoversSpider(scrapy.Spider):
    id = 4
    name = 'covers_mlb_line_history_with_score'
    download_delay = 0
    start_urls = ['http://www.covers.com/']

    def start_requests(self):
        yield scrapy.Request('http://www.covers.com/sports/mlb/matchups', self.parse)
    
    def parse(self, response):
        logging.info('Just arrived at %s' % response.url)               
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
    
    # Parsing line history page urls
    def parse_line_history_url(self, response):
        season = response.meta.get('season')
        print("at parse_line_history_url season and url ", season, response.url)
        logging.info('Just arrived at parse_line_history_url and url %s' % response.url)     
        game_boxes = response.xpath("//div[@class='cmg_matchups_list']//div[@class='cmg_matchup_game_box']").extract()
        print(len(game_boxes))
        for box in game_boxes:
            url = Selector(text=box).xpath("//div[@class='cmg_l_row cmg_matchup_list_gamebox']/a[text()='Line History']/@href").extract()
            team_a_name = Selector(text=box).xpath("//div[@class='cmg_matchup_list_column_1']/div[@class='cmg_team_name']/text()").extract()
            team_a_score = Selector(text=box).xpath("//div[@class='cmg_matchup_list_score']//div[1]/text()").extract()
            team_b_name = Selector(text=box).xpath("//div[@class='cmg_matchup_list_column_3']/div[@class='cmg_team_name']/text()").extract()
            team_b_score =  Selector(text=box).xpath("//div[@class='cmg_matchup_list_score']//div[3]/text()").extract()
            print(url)
            print(team_a_name + team_a_score)
            print(team_b_name +  team_b_score)
            yield scrapy.Request(url[0], self.parse_line_history, 
            meta={'season': season, 'team_a_name': team_a_name, 'team_a_score': team_a_score, 
            'team_b_name': team_b_name, 'team_b_score': team_b_score})

    # Parsing html of line history page
    def parse_line_history(self, response):
        season = response.meta.get('season')
        team_a_name = response.meta.get('team_a_name')
        team_b_name = response.meta.get('team_b_name')
        team_a_score = response.meta.get('team_a_score')
        team_b_score = response.meta.get('team_b_score')
        print("at parse_line_history ", response.url)
        logging.info('Just arrived at parse_line_history and url %s' % response.url)
        item = CoversLineHistoryItem()
        item['team_a_name'] = team_a_name[0].strip()
        item['team_b_name'] = team_b_name[1].strip()
        if not team_a_score:
            item['team_a_score'] = team_a_score
        else:
            item['team_a_score'] = team_a_score[0]
        if not team_b_score:
            item['team_b_score'] = team_b_score
        else:
            item['team_b_score'] = team_b_score[0]
        #item['team_a_score'] = team_a_score[0]
        #item['team_b_score'] = team_b_score[0]
        item['season'] = season
        team = response.xpath("//div[@class='leftCol']//h3/text()").extract()
        if not team:
            item['team_name'] = team
        else:
            item['team_name'] = team[0].replace('\r\n','')		
        match_date_time = response.xpath("//div[@class='leftCol']//h3/div[@class='right']/text()").extract()
        if not match_date_time:
            item['match_date_time'] = ""
        else:
            item['match_date_time'] = match_date_time[0]
        reference_website_url_0 = ""
        reference_website_name_0 = ""
        trs = response.xpath("//table[@id='bodyContentPlaceHolder_ucLineHistory_tblLineHistory']//tr").extract()
        for tr in trs:
            td = Selector(text=tr).xpath('//td').extract()
            reference_website_url = Selector(text=td[0]).xpath("//td/a/@href").extract()
            reference_website_name = Selector(text=td[0]).xpath("//td/a/text()").extract()
            if not reference_website_url:
                datetime = Selector(text=td[0]).xpath('//td/text()').extract()
                date_time = datetime[0].split()
                date = date_time[0]
                time = date_time[1] + date_time[2]
                item['date'] = date
                item['time'] = time
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
                
            

        

    

