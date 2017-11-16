# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import sys
import MySQLdb
import hashlib
from scrapy.exceptions import DropItem
from scrapy.http import Request
from mcrg.items import SpiderItem

import time
import datetime

ts = time.time()
timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

spider_activity_id = 0
inserted_trend = 1

class MySQLStorePipeline(object):
    def __init__(self):
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        dispatcher.connect(self.spider_error, signals.spider_error)
        self.conn = MySQLdb.connect('devmcrg-cluster.cluster-c3tzdaunyexk.us-west-2.rds.amazonaws.com',
                                    'devmcrg', 'O8XsAY1e', 'devmcrg', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
    
    # When spider open it will add entry into activity log table
    def spider_opened(self, spider):
        spider.logger.info('Spider opened id: %s', spider.id)
        spider.logger.info('Spider opened name: %s', spider.name)        
        self.cursor.execute("""INSERT INTO spider_activity (spider_id, started_at)  
                                VALUES ('%s', '%s')""" %
                                (spider.id, timestamp))
        global spider_activity_id
        spider_activity_id = self.cursor.lastrowid
        self.conn.commit() 

    
    # When spider close it will add entry into activity log table
    def spider_closed(self, spider):
        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        spider.logger.info('Spider closed: %s', spider.name)
        query = """UPDATE spider_activity SET finished_at = '%s'""" % timestamp + """, activity_log = 'Spider finished scusscessfully' WHERE id = %d""" % spider_activity_id
        self.cursor.execute(query)
        self.conn.commit()  
    
    def process_item(self, item, spider):
        self.spider = spider
        item['date_created'] = timestamp
        item['created_by'] = self.spider.name
        item['date_modified'] = timestamp
        item['modified_by'] = self.spider.name        

         # 1. ZCode, this is to insert into tables
        if self.spider.name in ['reads3html_spider']:  
            # 1.1 Check if system already exists in db, If yes then get id otherwise add
            print("========================")
            print("System name: '%s'" % item['system_name'])
            print("========================")
            self.cursor.execute("""SELECT id FROM zcode_system WHERE name = '%s'""" % item['system_name'])
            system_results = self.cursor.fetchone()
            if not system_results:
                print("========================")
                print('System does not exist, adding new system')
                print("========================")
                self.cursor.execute("""INSERT INTO zcode_system (name, date_created, created_by, date_modified, modified_by)  
                                    VALUES ('%s', '%s', '%s', '%s', '%s')""" % (item['system_name'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))
                item['system_id'] = self.cursor.lastrowid
                self.conn.commit()
            else:
                print("========================")
                print("system_id:", system_results[0])
                print("========================")
                item['system_id'] = system_results[0]

            # 1.2. Check if trend exists in db, If yes then get id otherwise add
            print("========================")
            print("Trend name: '%s'" % item['trend_name'])
            print("========================")
            self.cursor.execute(
                """SELECT id FROM zcode_system_trend WHERE name = '%s'""" % item['trend_name'])
            trend_results = self.cursor.fetchone()

            if not trend_results:
                print("========================")
                print('Trend does not exist, adding new trend')
                print("========================")
                self.cursor.execute("""INSERT INTO zcode_system_trend (name, system_id, date_created, created_by, date_modified, modified_by)  
                                        VALUES ('%s', %d,'%s' ,'%s' ,'%s' ,'%s')""" % (item['trend_name'], item['system_id'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))
                item['trend_id'] = self.cursor.lastrowid
                self.conn.commit()
            else:
                print("========================")
                print("trend_id:", trend_results[0])
                print("========================")
                item['trend_id'] = trend_results[0]
                     
            print("========================")
            print("table_name:", item['table_name'])
            print("========================")

            # 1.3. Insert values into the table
            self.cursor.execute("""INSERT INTO """ + item['table_name'] + """ (spider_activity_id, trend_id, row_number, value, bet_win_count, bet_win_percentage, bet_loss_count, bet_loss_percentage, bet_push_count, bet_push_percentage, bet_total_count, bet_total_percentage, profit, date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, %s, %s, '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', '%s', '%s')""" %
                                (spider_activity_id, item['trend_id'], item['row_number'], item['value'], item['bet_win_count'], item['bet_win_percentage'], item['bet_loss_count'], item['bet_loss_percentage'], item['bet_push_count'], item['bet_push_percentage'], item['bet_total_count'], item['bet_total_percentage'], item['profit'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))

            self.conn.commit()        

        # 2. ZCode this is for save zcode html to s3 storage.
        if self.spider.name in ['zcodehtmltos3_spider']:
            self.cursor.execute("""SELECT id FROM zcode_system_html_path WHERE html_file_url = '%s'""" % item['html_file_url'])
            results = self.cursor.fetchone()
            if not results:
                print("========================")
                print('Url does not exist, adding new s3 html file url')
                print("========================")
                self.cursor.execute("""INSERT INTO zcode_system_html_path (spider_activity_id, page_url, html_file_url, date_created, created_by)  
                                VALUES (%s, '%s', '%s','%s' ,'%s')""" %
                                (spider_activity_id, item['page_url'], item['html_file_url'], item['date_created'], item['created_by']))

                self.conn.commit()  
            else:
                print("========================")
                print("Updated activity id, record id:",spider_activity_id, results[0], item['date_modified'], item['modified_by'])
                print("========================")
                try:
                    query = """UPDATE zcode_system_html_path SET spider_activity_id = %s """ % spider_activity_id + """, date_modified = '%s'""" % item['date_modified'] + """, modified_by = '%s'""" % item['modified_by'] + """ WHERE id = %d""" % results[0]
                    self.cursor.execute(query)
                    self.conn.commit()
                except:
                    print(self.cursor._last_executed)
                    raise
    
        # 3. Covers.com line history push to db
        if self.spider.name in ['covers_mlb_line_history']:
            print("it's covers.com")
            # 3.1. Insert values into the table
            self.cursor.execute("""SELECT id FROM covers_mlb_team WHERE name = '%s' """ % item['team_name'] + """ and match_date_time = '%s'""" % item['match_date_time'])
            mlb_team_results = self.cursor.fetchone()
            if not mlb_team_results:
                print("========================")
                print('teams does not exist, adding new team')
                print("========================")
                self.cursor.execute("""INSERT INTO covers_mlb_team (spider_activity_id, name, match_date_time, season, date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" %
                                (spider_activity_id, item['team_name'], item['match_date_time'], item['season'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))
                item['team_id'] = self.cursor.lastrowid
                self.conn.commit()
            else:
                print("========================")
                print("team_id:", mlb_team_results[0])
                print("========================")
                item['team_id'] = mlb_team_results[0]
            
            self.cursor.execute("""INSERT INTO covers_mlb_score_matchups_line_history (team_id, reference_website_name, reference_website_url, time, line, over_under, date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" %
                                (item['team_id'], item['reference_website_name'], item['reference_website_url'], item['time'], item['line'], item['over_under'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))

            self.conn.commit()   

         # 4. Covers.com line history push to db with scores
        if self.spider.name in ['covers_mlb_line_history_with_score']:
            print("it's covers.com")
            # 4.1. Insert values into the table
            self.cursor.execute("""SELECT id FROM covers_mlb_team WHERE name = '%s' """ % item['team_name'] + """ and match_date_time = '%s'""" % item['match_date_time'] + """ and season = '%s'""" % item['season']) 
            mlb_team_results = self.cursor.fetchone()
            if not mlb_team_results:
                print("========================")
                print('teams does not exist, adding new team')
                print("========================")
                self.cursor.execute("""INSERT INTO covers_mlb_team (spider_activity_id, name, match_date_time, season, team_a_name, team_b_name, team_a_score, team_b_score, date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" %
                                (spider_activity_id, item['team_name'], item['match_date_time'], item['season'], item['team_a_name'], item['team_b_name'], item['team_a_score'], item['team_b_score'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))
                item['team_id'] = self.cursor.lastrowid
                self.conn.commit()
            else:
                print("========================")
                print("team_id:", mlb_team_results[0])
                print("========================")
                item['team_id'] = mlb_team_results[0]
            
            self.cursor.execute("""INSERT INTO covers_mlb_score_matchups_line_history (team_id, reference_website_name, reference_website_url, date, time, line, over_under, date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" %
                                (item['team_id'], item['reference_website_name'], item['reference_website_url'], item['date'], item['time'], item['line'], item['over_under'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))

            self.conn.commit()   

        # 5. Covers.com live scores
        if self.spider.name in ['covers_mlb_live_score']:
            print("it's covers.com live scores")
            ts = time.time()
            match_date = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
            match_time = datetime.datetime.fromtimestamp(ts).strftime('%H:%M:%S')
            # 5.1. Insert values into the table         
            self.cursor.execute("""INSERT INTO covers_mlb_live_score (spider_activity_id, team_name_at, league_name, team_a_name, 
            team_a_name_id, team_a_player_name, team_a_player_status, team_a_score, 
            team_b_name, team_b_name_id, team_b_score, team_b_player_name,
            team_b_player_status, season, team_a_1, team_a_2,
            team_a_3, team_a_4, team_a_5, team_a_6,
            team_a_7, team_a_8, team_a_9, team_a_r,
            team_a_ml, team_a_ou, team_a_h, team_a_e,
            team_b_1, team_b_2,
            team_b_3, team_b_4, team_b_5, team_b_6,
            team_b_7, team_b_8, team_b_9, team_b_r,
            team_b_ml, team_b_ou, team_b_h, team_b_e,
            match_date, match_time, 
            date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s', 
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',                                 
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s')""" %
                                (spider_activity_id, item['team_name_at'], item['league_name'], item['team_a_name'], 
                                item['team_a_name_id'], item['team_a_player_name'], item['team_a_player_status'], item['team_a_score'],
                                item['team_b_name'], item['team_b_name_id'], item['team_b_score'], item['team_b_player_name'],
                                item['team_b_player_status'], item['season'], item['team_a_1'], item['team_a_2'],
                                item['team_a_3'], item['team_a_4'], item['team_a_5'], item['team_a_6'],
                                item['team_a_7'], item['team_a_8'], item['team_a_9'], item['team_a_r'],
                                item['team_a_ml'], item['team_a_ou'], item['team_a_h'], item['team_a_e'],
                                item['team_b_1'], item['team_b_2'],
                                item['team_b_3'], item['team_b_4'], item['team_b_5'], item['team_b_6'],
                                item['team_b_7'], item['team_b_8'], item['team_b_9'], item['team_b_r'],
                                item['team_b_ml'], item['team_b_ou'], item['team_b_h'], item['team_b_e'],
                                match_date, match_time,
                                item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))

            self.conn.commit() 
			
		# 6. Oddsportal.com
        if self.spider.name in ['oddsportal_spider']:
            print("it's oddsportal live scores")
			# 6.1. Insert values into the table         
            self.cursor.execute("""INSERT INTO oddsportal_soccer (spider_activity_id, game_name,country_name, league_name, league_year,                  
            match_date, match_time, team_a_name, team_b_name,
            team_a_score, team_b_score, team_won, one,
            x, two, bs, url,            			
            team_a_1st_half, team_b_1st_half, team_a_2nd_half, team_b_2nd_half,
            pinnacle_1, pinnacle_x, pinnacle_2, pinnacle_payout,			
            date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s','%s',
								 '%s', '%s', '%s', '%s',                  
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',                                 
                                 '%s', '%s', '%s', '%s')""" %
                                (spider_activity_id, item['Game'], item['Country'], item['League'], 
                                item['League Year'], item['Game Date'], item['Game Time'], item['Team A'],
                                item['Team B'], item['Team A Score'], item['Team B Score'], item['Team Won'],
                                item['1'], item['X'], item['2'], item['Bs'],
                                item['url'], item['team_a_1st_half'], item['team_b_1st_half'], item['team_a_2nd_half'],
                                item['team_b_2nd_half'], item['pinnacle_1'], item['pinnacle_x'], item['pinnacle_2'],
                                item['pinnacle_payout'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))

            self.conn.commit()
    
        # 7. Oddsportal.com dialy
        if self.spider.name in ['oddsportal_daily']:
            print("it's oddsportal live scores")
			# 6.1. Insert values into the table         
            self.cursor.execute("""INSERT INTO oddsportal_soccer (spider_activity_id, game_name,country_name, league_name, league_year,                  
            match_date, match_time, team_a_name, team_b_name,
            team_a_score, team_b_score, team_won, one,
            x, two, bs, url,            			
            team_a_1st_half, team_b_1st_half, team_a_2nd_half, team_b_2nd_half,
            pinnacle_1, pinnacle_x, pinnacle_2, pinnacle_payout,			
            date_created, created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s','%s',
								 '%s', '%s', '%s', '%s',                  
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',                                 
                                 '%s', '%s', '%s', '%s')""" %
                                (spider_activity_id, item['Game'], item['Country'], item['League'], 
                                item['League Year'], item['Game Date'], item['Game Time'], item['Team A'],
                                item['Team B'], item['Team A Score'], item['Team B Score'], item['Team Won'],
                                item['1'], item['X'], item['2'], item['Bs'],
                                item['url'], item['team_a_1st_half'], item['team_b_1st_half'], item['team_a_2nd_half'],
                                item['team_b_2nd_half'], item['pinnacle_1'], item['pinnacle_x'], item['pinnacle_2'],
                                item['pinnacle_payout'], item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))

            self.conn.commit()
        # 8. Zcode Past picks dialy
        if self.spider.name in ['zcode_sports_trader_past_picks_spider']:
            #print("it's past picks")
			# 6.1. Insert values into the table 
            #self.cursor.execute("""SELECT id FROM zcode_sports_trader_past_picks WHERE system_name = '%s' """ % item['system_name'] + """ and trend_name = '%s'""" % item['trend_name'] + """ and game_date = '%s'""" % item['game_date'] + """ and team_a = '%s'""" % item['team_a']+ """ and team_b = '%s'""" % item['team_b'] + """ and bet_amount = '%s'""" % item['bet_amount'])           
            #zcode_game_results = self.cursor.fetchone()
            #if not zcode_game_results:
            global inserted_trend
			#global spider_activity_id
            #spider_activity_id = self.cursor.lastrowid
            if  item["trend_count"] >inserted_trend :
                print("========================")
                print(' Adding   HTML OF TREND  !!')
                print("========================")
                inserted_trend = inserted_trend+1
                self.cursor.execute("""INSERT INTO zcode_sports_trader_picks_html (
                spider_activity_id,html,type,date_created,
                created_by, date_modified, modified_by,system_name,trend_name)  
                            VALUES (%s, '%s', '%s',
                                    '%s', '%s','%s', '%s','%s','%s')""" %
                            (spider_activity_id, item['html'],'Past Picks',						
                            item['date_created'], item['created_by'], 
							item['date_modified'], item['modified_by'],item['system_name'], item['trend_name']))
                self.conn.commit()
            print("========================")
            print('Adding new Record !!')
            print("========================")
            self.cursor.execute("""INSERT INTO zcode_sports_trader_past_picks (
            spider_activity_id, bet_status, bet_team_name, system_name,
            trend_name, rotational_number, bet_on, odds,units,
            bet_amount, game_name, game_date, game_time,
            team_a, league_name,team_b,summary,date_created, 
            created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s', 
								    '%s', '%s', '%s', '%s','%s', 
									'%s', '%s', '%s', '%s','%s', 
									'%s', '%s', '%s', '%s',
									'%s','%s','%s')""" %
                                (spider_activity_id, item['bet_status'],item['bet_team_name'],
								item['system_name'], item['trend_name'], item['rotational_number'], item['bet_on'],
								item['odds'],item['units'], item['bet_amount'], item['game_name'], item['game_date'],
								item['game_time'], item['team_a'], item['league_name'], item['team_b'],item['summary'],							
								item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))
            self.conn.commit()
           #else:
                #print("========================")
                #print("team_id:", mlb_team_results[0])
                #print("Record Already Exist !!!")
                #print("========================")
                #item['team_id'] = mlb_team_results[0]        
            #self.conn.commit()	
			
	    # 9. Zcode Live picks dialy
        if self.spider.name in ['zcode_sports_trader_live_picks_spider']:		
            #global inserted_trend
            if  item["trend_count"] >inserted_trend :
                print("========================")
                print(' Adding   HTML OF TREND  !!')
                print("========================")
                inserted_trend = inserted_trend+1
                self.cursor.execute("""INSERT INTO zcode_sports_trader_picks_html (
                spider_activity_id,html,type,date_created,
                created_by, date_modified, modified_by,system_name,trend_name)  
                            VALUES (%s, '%s', '%s',
                                    '%s', '%s','%s', '%s','%s','%s')""" %
                            (spider_activity_id, item['html'],'Active Picks',						
                            item['date_created'], item['created_by'], 
							item['date_modified'], item['modified_by'],item['system_name'], item['trend_name']))
                self.conn.commit()
            print("========================")
            print('Adding new Record !!')
            print("========================")
            print("========================")
            print('Adding new Record !!')
            print("========================")
            self.cursor.execute("""INSERT INTO zcode_sports_trader_active_picks (
            spider_activity_id, bet_status, bet_team_name, system_name,
            trend_name, rotational_number, bet_on, odds,units,
            bet_amount, game_name, game_date, game_time,
            team_a, league_name,team_b,summary,date_created, 
            created_by, date_modified, modified_by)  
                                VALUES (%s, '%s', '%s', '%s', 
								    '%s', '%s', '%s', '%s','%s', 
									'%s', '%s', '%s', '%s','%s', 
									'%s', '%s', '%s', '%s',
									'%s','%s','%s')""" %
                                (spider_activity_id, item['bet_status'],item['bet_team_name'],
								item['system_name'], item['trend_name'], item['rotational_number'], item['bet_on'],
								item['odds'],item['units'], item['bet_amount'], item['game_name'], item['game_date'],
								item['game_time'], item['team_a'], item['league_name'], item['team_b'],item['summary'],							
								item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))
            self.conn.commit()

         # 10. Oddsportal.com
        if self.spider.name in ['oddsportal_tennis_spider']:
            print("it's oddsportal Tennis")
			# 6.1. Insert values into the table         
            self.cursor.execute("""INSERT INTO oddsportal_tennis (`spider_activity_id`, `game`,`1`, `2`, `bs`,                  
            `url`, `prize_money`, `player_1`, `player_2`,
            `league`, `league_year`,`player_won`, `country`, `game_date`,
            `game_time`, `set1_player_1`, `set1_player_2`, `set2_player_1`,            			
            `set2_player_2`, `set3_player_1`, `set3_player_2`, `pinnacle_1`,
            `pinnacle_2`, `pinnacle_payout`, `date_created`, `created_by`,			
            `date_modified`, `modified_by`)  
                                VALUES (%s, '%s', '%s', '%s','%s',
								 '%s', '%s', '%s', '%s',                  
                                 '%s', '%s', '%s', '%s','%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',
                                 '%s', '%s', '%s', '%s',                                 
                                 '%s', '%s')""" %
                                (spider_activity_id, item['Game'], item['1'], item['2'], 
                                item['Bs'], item['url'], item['Prize Money'], item['Player 1'],
                                item['Player 2'], item['League'], item['League Year'], item['Player Won'],
                                item['Country'], item['Game Date'], item['Game Time'], item['Set1 Player 1'],
                                item['Set1 Player 2'], item['Set2 Player 1'], item['Set2 Player 2'], item['Set3 Player 1'],
                                item['Set3 Player 2'], item['pinnacle_1'], item['pinnacle_2'], item['pinnacle_payout'],
                                item['date_created'], item['created_by'], item['date_modified'], item['modified_by']))

            self.conn.commit()			

    # When an error occur it will log error into table and time
    def spider_error(self, failure, response, spider):
        spider.logger.info('Spider error: %s', spider.name)
        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        query = """UPDATE spider_activity SET finished_at = '%s'""" % timestamp + """, activity_log = 'Error message is %s'""" % failure + """ WHERE id = %d""" % spider_activity_id
        self.cursor.execute(query)
        self.conn.commit()

