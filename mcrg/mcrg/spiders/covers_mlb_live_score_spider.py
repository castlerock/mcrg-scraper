import scrapy
from scrapy.selector import Selector
from mcrg.items import CoversLiveScoreItem
import logging
from scrapy.utils.log import configure_logging
from datetime import datetime, timedelta

configure_logging(install_root_handler=False)
logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO)

class CoversSpider(scrapy.Spider):
    id = 5
    name = 'covers_mlb_live_score'
    download_delay = 0
    start_urls = ['http://www.covers.com/']

    def start_requests(self):
        yield scrapy.Request('http://www.covers.com/sports/mlb/matchups', self.parse)
    
    def parse(self, response):
        logging.info('Just arrived at %s' % response.url)    
        game_boxes = response.xpath("//div[@class='cmg_matchups_list']//div[@class='cmg_matchup_game_box']").extract()
        for box in game_boxes:
            item = CoversLiveScoreItem()
            item['season'] = '2017'
            team_name_at = Selector(text=box).xpath("//div[@class='cmg_matchup_header_team_names']/text()").extract()
            item['team_name_at'] =team_name_at[0].strip()
            league_name = Selector(text=box).xpath("//div[@class='cmg_matchup_header_conference']/text()").extract()
            item['league_name'] = league_name[0].strip()

            team_a_name = Selector(text=box).xpath("//div[@class='cmg_matchup_list_column_1']/div[@class='cmg_team_name']/text()").extract()
            team_a_name_id = Selector(text=box).xpath("//div[@class='cmg_matchup_list_column_1']/div[@class='cmg_team_name']/span/text()").extract()
            team_a_score = Selector(text=box).xpath("//div[@class='cmg_matchup_list_score']//div[1]/text()").extract()
            item['team_a_name'] = team_a_name[0].strip()
            item['team_a_name_id'] = team_a_name_id[0].strip()
            if not team_a_score:
                item['team_a_score'] = 0
            else:
                item['team_a_score'] = team_a_score[0]

            team_b_name = Selector(text=box).xpath("//div[@class='cmg_matchup_list_column_3']/div[@class='cmg_team_name']/text()").extract()
            team_b_name_id = Selector(text=box).xpath("//div[@class='cmg_matchup_list_column_3']/div[@class='cmg_team_name']/span/text()").extract()
            team_b_score =  Selector(text=box).xpath("//div[@class='cmg_matchup_list_score']//div[3]/text()").extract()

            item['team_b_name'] = team_b_name[0].strip()
            item['team_b_name_id'] = team_b_name_id[0].strip()
            if not team_b_score:
                item['team_b_score'] = 0
            else:
                item['team_b_score'] = team_b_score[0]

            team_a_player_name = Selector(text=box).xpath("//div[@class='cmg_l_row']/div[1]/div[@class='player_status']/text()").extract()
            team_a_player_status = Selector(text=box).xpath("//div[@class='cmg_l_row']/div[1]/div[@class='player_status']/span/text()").extract()
            if not team_a_player_name:
                print("This is not a live game!")
            else:  
                print("This is live game ", team_a_name[0] + " vs ", team_b_name[1])              
                # print(team_a_player_name[0] ,team_a_player_status[0])
                if not team_a_player_name:
                    item['team_a_player_name'] = ""
                    item['team_a_player_status'] = ""
                else:
                    item['team_a_player_name'] = team_a_player_name[0].strip()
                    item['team_a_player_status'] = team_a_player_status[0].strip()

                team_b_player_name = Selector(text=box).xpath("//div[@class='cmg_l_row']/div[2]/div[@class='player_status']/span/text()").extract()
                team_b_player_status = Selector(text=box).xpath("//div[@class='cmg_l_row']/div[2]/div[@class='player_status']/text()").extract()
                if not team_b_player_name:
                    item['team_b_player_name'] = ""
                    item['team_b_player_status'] = ""
                else:
                    item['team_b_player_name'] = team_b_player_name[0].strip()
                    item['team_b_player_status'] = team_b_player_status[0].strip()
                
                line_scores = Selector(text=box).xpath("//div[@class='cmg_matchup_line_score']/table/tbody/tr").extract()
                for index, line in enumerate(line_scores):
                    if index == 0:
                        team_a_1 = Selector(text=line).xpath("//tr/td[2]/text()").extract()
                        team_a_2 = Selector(text=line).xpath("//tr/td[3]/text()").extract()
                        team_a_3 = Selector(text=line).xpath("//tr/td[4]/text()").extract()
                        team_a_4 = Selector(text=line).xpath("//tr/td[5]/text()").extract()
                        team_a_5 = Selector(text=line).xpath("//tr/td[6]/text()").extract()
                        team_a_6 = Selector(text=line).xpath("//tr/td[7]/text()").extract()
                        team_a_7 = Selector(text=line).xpath("//tr/td[8]/text()").extract()
                        team_a_8 = Selector(text=line).xpath("//tr/td[9]/text()").extract()
                        team_a_9 = Selector(text=line).xpath("//tr/td[10]/text()").extract()
                        team_a_r = Selector(text=line).xpath("//tr/td[11]/text()").extract()
                        team_a_ml = Selector(text=line).xpath("//tr/td[12]/text()").extract()
                        team_a_ou = Selector(text=line).xpath("//tr/td[13]/text()").extract()
                        team_a_h = Selector(text=line).xpath("//tr/td[14]/text()").extract()
                        team_a_e = Selector(text=line).xpath("//tr/td[15]/text()").extract()
                        print(team_a_1, team_a_2, team_a_3, team_a_4, team_a_5, team_a_6, team_a_7
                        , team_a_8, team_a_9, team_a_r, team_a_ml, team_a_ou, team_a_h, team_a_e)
                        item['team_a_1'] = team_a_1[0]
                        item['team_a_2'] = team_a_2[0]
                        item['team_a_3'] = team_a_3[0]
                        item['team_a_4'] = team_a_4[0]
                        item['team_a_5'] = team_a_5[0]
                        item['team_a_6'] = team_a_6[0]
                        item['team_a_7'] = team_a_7[0]
                        item['team_a_8'] = team_a_8[0]
                        item['team_a_9'] = team_a_9[0]
                        item['team_a_r'] = team_a_r[0]
                        item['team_a_ml'] = team_a_ml[0]
                        if not team_a_ou:
                            item['team_a_ou'] = '0'    
                        else:
                            item['team_a_ou'] = team_a_ou[0]
                        item['team_a_h'] = team_a_h[0]
                        item['team_a_e'] = team_a_e[0]
                    else:
                        team_b_1 = Selector(text=line).xpath("//tr/td[2]/text()").extract()
                        team_b_2 = Selector(text=line).xpath("//tr/td[3]/text()").extract()
                        team_b_3 = Selector(text=line).xpath("//tr/td[4]/text()").extract()
                        team_b_4 = Selector(text=line).xpath("//tr/td[5]/text()").extract()
                        team_b_5 = Selector(text=line).xpath("//tr/td[6]/text()").extract()
                        team_b_6 = Selector(text=line).xpath("//tr/td[7]/text()").extract()
                        team_b_7 = Selector(text=line).xpath("//tr/td[8]/text()").extract()
                        team_b_8 = Selector(text=line).xpath("//tr/td[9]/text()").extract()
                        team_b_9 = Selector(text=line).xpath("//tr/td[10]/text()").extract()
                        team_b_r = Selector(text=line).xpath("//tr/td[11]/text()").extract()
                        team_b_ml = Selector(text=line).xpath("//tr/td[12]/text()").extract()
                        team_b_ou = Selector(text=line).xpath("//tr/td[13]/text()").extract()
                        team_b_h = Selector(text=line).xpath("//tr/td[14]/text()").extract()
                        team_b_e = Selector(text=line).xpath("//tr/td[15]/text()").extract()
                        print( team_b_1, team_b_2, team_b_3, team_b_4, team_b_5, team_b_6, team_b_7
                        , team_b_8, team_b_9, team_b_r, team_b_ml, team_b_ou, team_b_h, team_b_e)
                        if not team_b_1:
                            item['team_b_1'] = ''
                        else:
                            item['team_b_1'] = team_b_1[0]
                        item['team_b_2'] = team_b_2[0]
                        item['team_b_3'] = team_b_3[0]
                        item['team_b_4'] = team_b_4[0]
                        item['team_b_5'] = team_b_5[0]
                        item['team_b_6'] = team_b_6[0]
                        item['team_b_7'] = team_b_7[0]
                        item['team_b_8'] = team_b_8[0]
                        item['team_b_9'] = team_b_9[0]
                        item['team_b_r'] = team_b_r[0]
                        item['team_b_ml'] = team_b_ml[0]
                        if not team_b_ou:
                            item['team_b_ou'] = '0'    
                        else:
                            item['team_b_ou'] = team_b_ou[0]
                        item['team_b_h'] = team_b_h[0]
                        item['team_b_e'] = team_b_e[0]
                yield item