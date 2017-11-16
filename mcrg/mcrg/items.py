# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class McrgItem(scrapy.Item):
    # define the fields for your item here like:
    table_name = scrapy.Field()
    system_id = scrapy.Field()
    system_name = scrapy.Field()
    trend_id = scrapy.Field() #it's a relationship with trend
    trend_name = scrapy.Field()
    
    row_number = scrapy.Field()
    value = scrapy.Field()
    bet_win_count = scrapy.Field()
    bet_win_percentage = scrapy.Field()
    bet_loss_count = scrapy.Field()
    bet_loss_percentage = scrapy.Field()
    bet_push_count = scrapy.Field()
    bet_push_percentage = scrapy.Field()
    bet_total_count = scrapy.Field()
    bet_total_percentage = scrapy.Field()
    profit = scrapy.Field()
    date_created = scrapy.Field()
    created_by = scrapy.Field()
    date_modified = scrapy.Field()
    modified_by = scrapy.Field()
    pass

class SpiderItem(scrapy.Item):
    #id = scrapy.Field()
    name = scrapy.Field()   

class ZCodeSystemHTMLPathItem(scrapy.Item):
    #id = scrapy.Field()
    spider_activity_id = scrapy.Field()
    page_url = scrapy.Field()   
    html_file_url = scrapy.Field()
    is_parsed = scrapy.Field()
    date_created = scrapy.Field()
    created_by = scrapy.Field()
    date_modified = scrapy.Field()
    modified_by = scrapy.Field()


class CoversLineHistoryItem(scrapy.Item):
    spider_activity_id = scrapy.Field()
    team_id = scrapy.Field()
    team_name = scrapy.Field()
    team_a_name = scrapy.Field()
    team_b_name = scrapy.Field()
    team_a_score = scrapy.Field()
    team_b_score = scrapy.Field()
    match_date_time = scrapy.Field()
    season = scrapy.Field()
    reference_website_name = scrapy.Field()
    reference_website_url = scrapy.Field()
    date = scrapy.Field()
    time = scrapy.Field()
    line = scrapy.Field()
    over_under = scrapy.Field()
    date_created = scrapy.Field()
    created_by = scrapy.Field()
    date_modified = scrapy.Field()
    modified_by = scrapy.Field()

class CoversLiveScoreItem(scrapy.Item):
    spider_activity_id = scrapy.Field()
    team_name_at = scrapy.Field()
    league_name = scrapy.Field()
    team_a_name = scrapy.Field()
    team_a_name_id = scrapy.Field()
    team_a_player_name = scrapy.Field()
    team_a_player_status = scrapy.Field()
    team_a_score = scrapy.Field()
    team_b_name = scrapy.Field()
    team_b_name_id = scrapy.Field()    
    team_b_score = scrapy.Field()
    team_b_player_name = scrapy.Field()
    team_b_player_status = scrapy.Field()
    season = scrapy.Field() 
    team_a_1 = scrapy.Field()
    team_a_2 = scrapy.Field()
    team_a_3 = scrapy.Field()
    team_a_4 = scrapy.Field()
    team_a_5 = scrapy.Field()
    team_a_6 = scrapy.Field()
    team_a_7 = scrapy.Field()
    team_a_8 = scrapy.Field()
    team_a_9 = scrapy.Field()
    team_a_r = scrapy.Field()
    team_a_ml = scrapy.Field()
    team_a_ou = scrapy.Field()
    team_a_h = scrapy.Field()
    team_a_e = scrapy.Field()
    team_b_1 = scrapy.Field()
    team_b_2 = scrapy.Field()
    team_b_3 = scrapy.Field()
    team_b_4 = scrapy.Field()
    team_b_5 = scrapy.Field()
    team_b_6 = scrapy.Field()
    team_b_7 = scrapy.Field()
    team_b_8 = scrapy.Field()
    team_b_9 = scrapy.Field()
    team_b_r = scrapy.Field()
    team_b_ml = scrapy.Field()
    team_b_ou = scrapy.Field()
    team_b_h = scrapy.Field()
    team_b_e = scrapy.Field()
    # match_date = scrapy.Field()
    # match_time = scrapy.Field()
    date_created = scrapy.Field()
    created_by = scrapy.Field()
    date_modified = scrapy.Field()
    modified_by = scrapy.Field()

	
class OddsportalItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}

class ZcodeItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}

class ZcodePastPicksItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}
		
		
class ZcodeSystemsTrendsItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}
		
class ZcodeLivePicksItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}

class OddsportalITennistem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}
		