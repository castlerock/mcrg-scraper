import scrapy
import requests
import json
from scrapy.selector import Selector
from mcrg.items import ZcodeLivePicksItem
import logging
from scrapy.utils.log import configure_logging
from datetime import datetime, timedelta
from collections import namedtuple
import requests
from scrapy.selector import HtmlXPathSelector
import re

configure_logging(install_root_handler=False)
logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO)

class ZcodeSportsTraderLivePicksSpider(scrapy.Spider):
    id = 9
    name = 'zcode_sports_trader_live_picks_spider'
    download_delay = 2
    start_urls = ['http://zcodesystem.com/vipclub/login.php']
	
    def parse(self, response):
        return scrapy.FormRequest.from_response(
            response,
            formdata={'emailaddress': 'grao@castlerockresearch.com', 'password': 'Salmana29$'},
            callback=self.after_login
        )   
    def after_login(self, response):
        # check login succeed before going on
        if b"Password is incorrect" in response.body:
            self.logger.error("Login failed")
            return

        # continue scraping with authenticated session...
        if b"Welcome to the VIP Club" in response.body:
            request = scrapy.Request("http://zcodesystem.com/sports_trader/start.php", callback=self.parse_sportstrader)
            yield request

    # fetch all the html for passed trends       
    def parse_sportstrader(self, response):
        self.logger.info("Just arrived at %s", response.url)
		
		#fetch all the trends from live site 
        url = "http://zcodesystem.com/sports_trader/start.php"
        headers = {
          'cache-control': "no-cache"
         }
        response = requests.request("GET", url, headers=headers)
        jsonresponse = response.text
       
        ul = Selector(text = jsonresponse).xpath('//ul[@id="TrendList"]//li').extract()
        #array to store all trends 
        trend_list = []
        #text_file = open('outputActive.txt', 'w')
        for li in ul:
            trend_id = Selector(text=li).xpath('//@data-id').extract()
            #add each trend Id in the array 
            trend_list.append((trend_id)[0])
            trend_count=0;			
        for trend in trend_list:
            trend_count =trend_count+1
            #print(trend)
            #print("here")
            urls = "http://zcodesystem.com/sports_trader/get_active_signals.php"
            payload = {'sports': '[""]','trends': '['+str(trend)+']','typecalc': 'unitsize','unitsize': '100','asort_mode': '0','psort_mode': '0','asignals': '[]'}
            headers = {
            'origin': "http://zcodesystem.com",
            'user-agent': "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            'content-type': "application/x-www-form-urlencoded",
            'accept': "text/html, */*; q=0.01",
            'x-devtools-emulate-network-conditions-client-id': "e5b8391b-5289-4df6-9973-4e2c6d2dc1f1",
            'x-requested-with': "XMLHttpRequest",
            'x-devtools-request-id': "5516.3762",
            'referer': "http://zcodesystem.com/sports_trader/start.php",
            'accept-encoding': "gzip, deflate",
            'accept-language': "en-US,en;q=0.8",	
            'cookie': "ZC_LANG=en; referrer=27.255.222.81%3Azcodesystem.com%252Fsports_trader%252Fstart.php%3Azcodesystem.com%252Fvipclub%252Fdo_login_ns; floaddt=1501734547; mp_6b62034815c12d29db73e6f6fcbd92c2_mixpanel=%7B%22distinct_id%22%3A%20%2215da65ac858129-01077374a2c639-474a0721-100200-15da65ac859ee%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D; phpbb3_1wd74_u=1; phpbb3_1wd74_k=; phpbb3_1wd74_sid=197df12ae45695aeb5c5c06b5b916711; _ga=GA1.2.2077156676.1501656646; _gid=GA1.2.558992684.1501832117; sessid2=sessid20170731061749636; PHPSESSID=b0208e1960e20894bfe171532904fcea; show_club_menu=0; show_picks_menu=1; ST_APP_MODE=Hbd6eooh",
            'cache-control': "no-cache"
            }
            response = requests.request("POST", urls, data=payload, headers=headers)
            jsonresponse = response.json()
            jsonstr = jsonresponse["active_html"]
            #text_file.write(jsonstr) 
            #print(jsonstr)	
            #			
            trs = Selector(text = jsonstr).xpath('//table[contains(@class, "ActiveSignals") and contains(@class, "actual")]//tr').extract()
            if trs:
                item = {}
                for tr in trs:
                    td = Selector(text=tr).xpath('//td').extract()
                    bet_status = ''.join(Selector(text=td[0]).xpath('//td/div[@class="iplaced"]/span[contains(@class, "bet_result-1") or contains(@class, "bet_result1")]/text()').extract())
                    system_name=''.join(Selector(text=td[0]).xpath('//td//div[contains(@class, "left_shift") and contains(@class, "system_name")]/text()').extract())
                    trend_name=''.join(Selector(text=td[0]).xpath('//td//div[@class="left_shift"][1]/text()').extract())
                    bet_team_name=''.join(Selector(text=td[0]).xpath('//td//div[@class="left_shift"]/div[@class="teamName"]//div[@class="Text"]/text()').extract())
                    rotational_number=''.join(Selector(text=td[0]).xpath('//td//div[@class="left_shift"]/div[@class="teamName"]//div[@class="Text"]/div[@class="rot_num"]/text()').extract())
                    item['bet_status'] = bet_status
                    item['system_name'] = system_name.replace("'","")
                    item['trend_name'] = trend_name.replace("'","")
                    item['bet_team_name'] = bet_team_name
                    item['rotational_number'] = rotational_number
                    bet_on=''.join(Selector(text=tr).xpath('(//td[@class="beton_col"]//text())[7]').extract())
                    item['bet_on'] = bet_on           
                    odds=Selector(text=tr).xpath('//td[@class="odd"]/text()').extract()
                    item['odds'] = ''.join(odds)          
                    units=''.join(Selector(text=tr).xpath('//td[@class="unit"]/text()').extract())
                    item['units'] = ''.join(units)
                    bet_amount=''.join(Selector(text=tr).xpath('//td[@class="bet"]/text()').extract())
                    item['bet_amount'] = ''.join(bet_amount)
                    game_name=''.join(Selector(text=tr).xpath('//td[@class="game_col"]/strong/text()').extract())
                    item['game_name'] = ''.join(game_name)
                    game_details=''.join(Selector(text=tr).xpath('//td[@class="game_col"]//text()').extract())
                    game_details=game_details.replace(game_name,"")
                    game_time=game_details[game_details.find(",")+1:game_details.find(")")]
                    item['game_time'] = ''.join(game_time)
                    league_start_point= game_details.rfind("(")
                    league_end_point=game_details.rfind(")")
                    if league_start_point == game_details.find("(") :
                        league_name=''
                        team_b=game_details[game_details.find("-")+2:len(game_details)]
                    else:
                        #league_end_point=game_details.rfind(")")
                        league_name=game_details[league_start_point+1:league_end_point]
                        team_b=game_details[game_details.find("-")+2:league_start_point]                
				    #league_name=game_details[league_start_point+1:league_end_point]
                    item['league_name'] = ''.join(league_name)                
                    game_date=game_details[0:game_details.find(",")]
                    item['game_date'] = ''.join(game_date)			
                    team_a=game_details[game_details.find(")")+1:game_details.find("-")]
                    item['team_a'] = ''.join(team_a)
                    #team_b=game_details[game_details.find("-")+2:league_start_point]
                    item['team_b'] = ''.join(team_b)
                    item['summary']=game_details
                    item['html']=jsonstr.replace("'","")
                    item['trend_count']=trend_count
                    print("==========================")
                    print(item)
                    print("==========================")
                    yield item
            else:
                print("==========================")
                print("No Live Sgnal Found For This Trend !!!!!!!!!!!!!!")
                print("==========================")