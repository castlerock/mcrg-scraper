# -*- coding: utf-8 -*-
import re
import json
import time
import datetime
import urllib.parse
from urllib.parse import urljoin

import scrapy
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.shell import inspect_response

import traceback

from mcrg.items import OddsportalItem
#from oddsportal.items import OddsportalItem


class OddsportalTennisSpider(scrapy.Spider):
    id = 10
    name = "oddsportal_tennis_spider"
    allowed_domains = ["oddsportal.com"]
    start_urls = ['http://www.oddsportal.com']
    url = 'http://fb.oddsportal.com/ajax-sport-country-tournament-archive/1/{}/X0/1/{}/{}/?_={}'
    base_url = 'http://www.oddsportal.com'
    # need to provide a time offset where the spider is running
    # tried to get the offset from site itself but it always returns 1 or 0
    # since i have tested from India it should be 5.5,
    # I think it requires javascript to return the correct offset
    # so please provide correct offset here
    # you can use the below link to provide the proper offset based on
    # the location of your server where the spider is running
    # http://www.fileformat.info/system/timezone.htm
    offset = 5.5
    today = datetime.datetime.today()

    def parse(self, response):
        countries = response.xpath('//div[@id="s_2"]/ul/li[@class="country"]')
        # will start from index 1 onwards
        # index 0 is popular and index 1 is Argentina
        # for li in countries[1:2]:
        for li in countries:
            country = li.xpath('./a/span/text()').extract_first()
            print(country)
            if country:
                country = country.strip()
            tournaments = li.xpath('./ul/li[@class="tournament"]')
            for t_li in tournaments:
                meta_data = {}
                meta_data['country'] = country
                tournament_link = t_li.xpath('./a/@href').extract_first()
                tournament_name = t_li.xpath('./a/text()').extract_first()
                if tournament_name:
                    meta_data['tournament_name'] = tournament_name.strip()
                else:
                    meta_data['tournament_name'] = ""
                if tournament_link:
                    url = response.urljoin(tournament_link).rstrip('/') + '/results/'
                    yield Request(url=url, callback=self.parse_tournament, meta=meta_data, dont_filter=True)

    def get_param_id(self, response, key):
        script_data = response.xpath('//script[contains(text(), "%s({")]' % key).re(r'%s\((\{.*\})\)\;' % key)
        if script_data:
            data = json.loads(script_data[0])
        else:
            data = {}
        return data

    def parse_tournament(self, response):
        print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
        meta = response.meta
        country = meta.get("country")
        TOURNAMENT_YEAR_XPATH = '//div[@class="main-menu2 main-menu-gray"]/ul[@class="main-filter"]/li[span[@class="active"]]/span/strong/a/text()'
        PRIZE_MONEY_XPATH = '//div[@class="prizemoney"]/text()'
        tournament_name = meta.get("tournament_name")
        page = 1
        header = {"Referer": response.url}
        script_data = response.xpath('//script[contains(text(), "PageTournament({")]').re(r'PageTournament\((\{.*\})\)\;')
        tournament_year = response.xpath(TOURNAMENT_YEAR_XPATH).extract_first()
        tournament_year = tournament_year.strip() if tournament_year else ""
        prize_money = response.xpath(PRIZE_MONEY_XPATH).extract()
        prizemoney = prize_money[0].strip() if prize_money else ""
        meta_data = {"country": country, "tournament_name": tournament_name, "tournament_year": tournament_year, "prizemoney": prizemoney}
        if script_data:
            data = json.loads(script_data[0])
            param_id = data.get("id")
            meta_data = {"country": country, "tournament_name": tournament_name, "param_id": param_id, 'refer': response.url, "tournament_year": tournament_year, "prizemoney": prizemoney}
            # URL OF AJAX CALL TO POPULATE THE PAGE
            url = self.url.format(param_id, self.offset, page, repr(time.time()).replace('.', '')[:-3])
            yield Request(url=url, callback=self.parse_data, dont_filter=True, headers=header, meta=meta_data)

        history_data = response.xpath('//div[@class="main-menu2 main-menu-gray"]/ul[@class="main-filter"]/li[span[@class="inactive"]]/span/strong/a/@href').extract()
        for link in history_data:
            url = response.urljoin(link)
            yield Request(url=url, callback=self.parse_secondary_tournamenet, dont_filter=True, headers=header, meta=meta_data)

    def parse_secondary_tournamenet(self, response):
        meta = response.meta
        country = meta.get("country")
        tournament_name = meta.get("tournament_name")
        tournament_year = meta.get("tournament_year")
        prizemoney = meta.get("prizemoney")
        refer = meta.get("refer")
        page = 1
        header = {"Referer": response.url}
        TOURNAMENT_YEAR_XPATH = '//div[@class="main-menu2 main-menu-gray"]/ul[@class="main-filter"]/li[span[@class="active"]]/span/strong/a/text()'
        script_data = response.xpath('//script[contains(text(), "PageTournament({")]').re(r'PageTournament\((\{.*\})\)\;')
        tournament_year = response.xpath(TOURNAMENT_YEAR_XPATH).extract_first()
        tournament_year = tournament_year.strip() if tournament_year else ""
        if script_data:
            data = json.loads(script_data[0])
            param_id = data.get("id")
            meta_data = {'refer': refer, "param_id": param_id, "country": country, "tournament_name": tournament_name, "tournament_year": tournament_year, "prizemoney": prizemoney}
            # URL OF AJAX CALL TO POPULATE THE PAGE
            url = self.url.format(param_id, self.offset, page, repr(time.time()).replace('.', '')[:-3])
            yield Request(url=url, callback=self.parse_data, dont_filter=True, headers=header, meta=meta_data)

    def decrypt(self, string):
        '''
        Method to decript the values from html
        '''
        new_string = string.replace('a', '1').replace('x', '2').replace('c', '3').replace('t', '4').replace('e', '5').replace('o', '6').replace('p', '7').replace('z', '.').replace('f', '|')
        new_value = new_string.split("|")[-1]
        return new_value

    def calculate_payout(self, data_list):
        sum = 0
        for i in data_list:
            sum += 1 / i
        return round((1 / sum) * 100, 1)

    def parse_data(self, response):
        meta = response.meta
        referer = meta.get("refer")
        param_id = meta.get("param_id")
        headers = {"Referer": referer}
        prizemoney = meta.get("prizemoney")
        meta_data = {"refer": referer, "param_id": param_id, "tournament_name": meta.get("tournament_name"), "tournament_year": meta.get("tournament_year")}
        content = re.findall(r'(\{.*\})\)\;', response.body_as_unicode(), re.M)
        if content:
            content = json.loads(content[0])
            sel = Selector(text=content.get('d', {}).get("html"))

            # XPATHS
            trs = sel.xpath('//tr[@class="center nob-border"]')
            PARTICIPANTS_LINK_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="name table-participant"]/a/@href'
            VAR_ONE_XPATH = './self::tr/following-sibling::tr[%s]/td[4]/@xodd'
            VAR_TWO_XPATH = './self::tr/following-sibling::tr[%s]/td[5]/@xodd'
            VAR_B_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="center info-value"]/text()'
            for index, tr in enumerate(trs):
                counter = 0
                while True:
                    counter += 1
                    tr_class = tr.xpath('./self::tr/following-sibling::tr[%s]/@class' % counter).extract_first()
                    if tr_class == "center nob-border" or index == len(trs) - 1:
                        break
                    if tr_class == "table-dummyrow":
                        continue
                    if tr_class == "dark center":
                        continue
                    # extract the data
                    participants_link = tr.xpath(PARTICIPANTS_LINK_XPATH % counter).extract_first()
                    var_one = tr.xpath(VAR_ONE_XPATH % counter).extract()
                    var_two = tr.xpath(VAR_TWO_XPATH % counter).extract()
                    var_b = tr.xpath(VAR_B_XPATH % counter).extract()

                    # cleaning the data
                    match_link = urljoin(self.base_url, participants_link) if participants_link else ""
                    var_one = self.decrypt(var_one[0]) if var_one else ""
                    var_two = self.decrypt(var_two[0]) if var_two else ""
                    var_b = var_b[0] if var_b else ""
                    item = {}
                    item['Game'] = "Tennis"
                    item['1'] = var_one
                    item['2'] = var_two
                    item['Bs'] = var_b
                    item['url'] = match_link
                    item['Prize Money'] = prizemoney
                    next_meta = {"item": item}
                    yield Request(url=match_link, callback=self.parse_score, meta=next_meta)

            # next_page = sel.xpath(u'//a[span[contains(text(), "»")]]/@x-page').extract_first()
            final_page = sel.xpath(u'//a[span[contains(text(), "»|")]]/@x-page').extract_first()
            # if next_page < final_page:
            #    page = next_page.strip()
            if final_page:      # newly added
                for page in range(int(final_page)):  # newly added
                    url = self.url.format(param_id, self.offset, page, repr(time.time()).replace('.', '')[:-3])
                    yield Request(url=url, callback=self.parse_secondary_data, dont_filter=True, headers=headers, meta=meta_data)

    def parse_secondary_data(self, response):
        meta = response.meta
        referer = meta.get("refer")
        header = {"Referer": referer}
        param_id = meta.get("param_id")
        meta_data = {"refer": referer, "param_id": param_id}
        content = re.findall(r'(\{.*\})\)\;', response.body_as_unicode(), re.M)
        if content:
            content = json.loads(content[0])
            sel = Selector(text=content.get('d', {}).get("html"))

            # XPATHS
            trs = sel.xpath('//tr[@class="center nob-border"]')
            PARTICIPANTS_LINK_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="name table-participant"]/a/@href'
            VAR_ONE_XPATH = './self::tr/following-sibling::tr[%s]/td[4]/@xodd'
            VAR_TWO_XPATH = './self::tr/following-sibling::tr[%s]/td[5]/@xodd'
            VAR_B_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="center info-value"]/text()'
            for index, tr in enumerate(trs):
                counter = 0
                while True:
                    counter += 1
                    tr_class = tr.xpath('./self::tr/following-sibling::tr[%s]/@class' % counter).extract_first()
                    if tr_class == "center nob-border" or index == len(trs) - 1:
                        break
                    if tr_class == "table-dummyrow":
                        continue
                    if tr_class == "dark center":
                        continue
                    # extract the data
                    participants_link = tr.xpath(PARTICIPANTS_LINK_XPATH % counter).extract_first()
                    var_one = tr.xpath(VAR_ONE_XPATH % counter).extract()
                    var_two = tr.xpath(VAR_TWO_XPATH % counter).extract()
                    var_b = tr.xpath(VAR_B_XPATH % counter).extract()

                    # cleaning the data
                    match_link = urljoin(self.base_url, participants_link) if participants_link else ""
                    var_one = self.decrypt(var_one[0]) if var_one else ""
                    var_two = self.decrypt(var_two[0]) if var_two else ""
                    var_b = var_b[0] if var_b else ""
                    item = {}
                    item['1'] = var_one
                    item['2'] = var_two
                    item['Bs'] = var_b
                    item['url'] = match_link
                    next_meta = {"item": item}
                    yield Request(url=match_link, callback=self.parse_score, meta=next_meta)

    def parse_score(self, response):
        param_dict = {}
        url = 'http://fb.oddsportal.com/feed/match/{versionId}-{sportId}-{id}-3-2-{xhash}.dat?_='
        meta = response.meta
        header = {"Referer": response.url, 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36'}
        item = meta.get("item")
        PARTICIPANTS_XPATH = '//div[@id="col-content"]/h1/text()'
        BREADCRUMBS_XPATH = '//div[@id="breadcrumb"]/a/text()'
        RESULT_XPATH = '//div[@id="event-status"]/p[@class="result"]/strong/text()'
        SETS_XPATH = '//div[@id="event-status"]/p[@class="result"]/text()'
        DATE_TIME_XPATH = '//div[@id="col-content"]/p[contains(@class, "date datet")]/@class'
        param_dict = self.get_param_id(response, "PageEvent")
        if param_dict:
            param_dict['xhash'] = urllib.parse.unquote(param_dict.get("xhash"))
            participants = response.xpath(PARTICIPANTS_XPATH).extract()
            breadcrumbs = response.xpath(BREADCRUMBS_XPATH).extract()
            result = response.xpath(RESULT_XPATH).extract()

            player_1, player_2 = "", ""
            if participants:
                participants = participants[0].split(' - ')
                if len(participants) == 2:
                    player_1 = participants[0].strip()
                    player_2 = participants[1].strip()
            league = ""
            country = ""
            if len(breadcrumbs) == 4:
                league = breadcrumbs[-1]
                country = breadcrumbs[-2]
            year = re.findall(r'\d{4}', league)
            if year:
                year = year[0]
            else:
                self.today.strftime("%Y")
            player_1_sets_won, player_2_sets_won = "", ""
            if result:
                result = result[0].strip().split(":")
                if len(result) == 2:
                    player_1_sets_won, player_2_sets_won = result
            if player_1_sets_won > player_2_sets_won:
                winner = player_1
            elif player_1_sets_won < player_2_sets_won:
                winner = player_2
            else:
                winner = ""
            sets = response.xpath(SETS_XPATH).extract()
            set1_player1, set1_player2, set2_player1, set2_player2, set3_player1, set3_player2 = "", "", "", "", "", ""
            if sets:
                sets = ''.join(sets).strip().strip('(').strip(')').split(', ')
                if len(sets) == 3:
                    set1_player1, set1_player2 = sets[0].split(":")
                    set2_player1, set2_player2 = sets[1].split(":")
                    set3_player1, set3_player2 = sets[2].split(":")
                elif len(sets) == 2:
                    set1_player1, set1_player2 = sets[0].split(":")
                    set2_player1, set2_player2 = sets[1].split(":")
            _date = ""
            _time = ""
            date_time = response.xpath(DATE_TIME_XPATH).re('datet\st(\d+)')
            if date_time:
                match_data_time = datetime.datetime.fromtimestamp(int(date_time[0]))
                _date = match_data_time.strftime("%d %b %Y")
                _time = match_data_time.strftime("%H:%M")

            # item assignment
            item["Game"] = "Tennis"
            item["Player 1"] = player_1
            item["Player 2"] = player_2
            item['League'] = league
            item['League Year'] = year
            item['Player Won'] = winner
            item['Country'] = country
            item['Game Date'] = _date
            item['Game Time'] = _time
            item["Set1 Player 1"] = set1_player1
            item["Set1 Player 2"] = set1_player2
            item["Set2 Player 1"] = set2_player1
            item["Set2 Player 2"] = set2_player2
            item["Set3 Player 1"] = set3_player1
            item["Set3 Player 2"] = set3_player2
            meta_data = {"item": item}
            link = url.format(**param_dict)
            link = link + repr(time.time()).replace('.', '')[:-3]
            yield Request(url=link, callback=self.parse_final, meta=meta_data, headers=header)

    def parse_final(self, response):
        print('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')
        # inspect_response(response, self)
        # do not delete the following 3 variables
        # true , false and null
        true = True
        false = False
        null = ""
        data_item = response.meta.get('item')
        item = OddsportalItem(**data_item)
        content = re.findall(r'(\{.*\})\)\;', response.body_as_unicode(), re.M)
        if content:
            try:
                content = json.loads(content[0])
                back = content.get('d').get('oddsdata').get('back')
                back_key = list(content.get('d').get('oddsdata').get('back').keys())[0]
                # 18, is the vlaue for pinaccle,
                # check http://www.oddsportal.com/res/x/bookies-170712184507-1499865132.js
                odds = back.get(back_key).get('odds').get("18")
                pinnacle_1 = ""
                pinnacle_2 = ""
                payout = ""
                if odds:
                    if isinstance(odds, dict):
                        pinnacle_1 = odds.get("0")
                        pinnacle_2 = odds.get("1")
                    else:
                        pinnacle_1, pinnacle_2 = odds
                    data_list = [pinnacle_1, pinnacle_2]
                    payout = self.calculate_payout(data_list)
                item["pinnacle_1"] = pinnacle_1
                item["pinnacle_2"] = pinnacle_2
                item['pinnacle_payout'] = payout
                item['date_created']= ""
                item['created_by']= ""
                item['date_modified']= ""
                item['modified_by']= ""
                yield item
            except Exception as e:
                print(traceback.print_exc())
        else:
            print('>>>>>>>>>>>>>>>>>>>>>')