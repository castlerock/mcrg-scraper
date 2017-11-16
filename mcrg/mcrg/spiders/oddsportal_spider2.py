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

# from mcrg.items import OddsportalItem
from mcrg.items import OddsportalItem


class OddsportalSpiderSpiderz(scrapy.Spider):
    id = 6
    name = "oddsportal_spider"
    allowed_domains = ["oddsportal.com"]
    start_urls = ['http://www.oddsportal.com/soccer/africa/cosafa-cup/results/']
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

    def parse(self, response):
        countries = response.xpath('//li[@class="country"]')
        # will start from index 1 onwards
        # index 0 is popular and index 1 is Argentina
        # for li in countries[1:2]:
        for li in countries[1:]:
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
                    if tournament_link.count("/soccer/"):
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
        meta = response.meta
        country = meta.get("country")
        TOURNAMENT_YEAR_XPATH = '//div[@class="main-menu2 main-menu-gray"]/ul[@class="main-filter"]/li[span[@class="active"]]/span/strong/a/text()'
        tournament_name = meta.get("tournament_name")
        page = 1
        header = {"Referer": response.url}
        script_data = response.xpath('//script[contains(text(), "PageTournament({")]').re(r'PageTournament\((\{.*\})\)\;')
        tournament_year = response.xpath(TOURNAMENT_YEAR_XPATH).extract_first()
        tournament_year = tournament_year.strip() if tournament_year else ""
        if "2017" in tournament_year:
            meta_data = {"country": country, "tournament_name": tournament_name, "tournament_year": tournament_year}
            if script_data:
                data = json.loads(script_data[0])
                param_id = data.get("id")
                meta_data = {"country": country, "tournament_name": tournament_name, "param_id": param_id, 'refer': response.url, "tournament_year": tournament_year}
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
            meta_data = {'refer': refer, "param_id": param_id, "country": country, "tournament_name": tournament_name, "tournament_year": tournament_year}
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
        date_flag = True
        meta = response.meta
        referer = meta.get("refer")
        param_id = meta.get("param_id")
        country = meta.get("country")
        headers = {"Referer": referer}
        tournament_year = meta.get("tournament_year")
        tournament_name = meta.get("tournament_name")
        meta_data = {"refer": referer, "param_id": param_id, "country": country, "tournament_name": meta.get("tournament_name"), "tournament_year": meta.get("tournament_year")}
        content = re.findall(r'(\{.*\})\)\;', response.body_as_unicode(), re.M)
        if content:
            content = json.loads(content[0])
            sel = Selector(text=content.get('d', {}).get("html"))
            date_in_url = re.findall(r'(\d{4}\-?\d{0,4})\/results\/$', referer)
            if not date_in_url:
                tournament_year = meta.get("tournament_year")
                tournament_name = meta.get("tournament_name")
            else:
                date_flag = False
                bread_crumbs = sel.xpath('//tr[@class="dark center"][1]/th/a/text()').extract()
                league_year = bread_crumbs[-1].rsplit(' ', 1)
                tournament_name = league_year[0]
                tournament_year = league_year[1]

            # XPATHS
            trs = sel.xpath('//tr[@class="center nob-border"]')
            PARTICIPANTS_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="name table-participant"]/a//text()'
            PARTICIPANTS_LINK_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="name table-participant"]/a/@href'
            MATCH_DATE_TIME_XPATH = './self::tr/following-sibling::tr[%s]/td[contains(@class, "datet")]/@class'
            SCORE_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="center bold table-odds table-score"]//text()'
            VAR_ONE_XPATH = './self::tr/following-sibling::tr[%s]/td[4]/@xodd'
            VAR_X_XPATH = './self::tr/following-sibling::tr[%s]/td[5]/@xodd'
            VAR_TWO_XPATH = './self::tr/following-sibling::tr[%s]/td[6]/@xodd'
            VAR_B_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="center info-value"]/text()'
            for index, tr in enumerate(trs):
                counter = 0
                header = tr.xpath('./self::tr/th[@class="first2 tl"]/text()').extract_first()
                if not date_flag:
                    th_data = tr.xpath('./self::tr/preceding-sibling::tr[@class="dark center"][1]/th/a/text()').extract()
                    info = th_data[-1].rsplit(' ', 1)
                    tournament_year = info[1].strip()
                    tournament_name = info[0].strip()
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
                    match_date_time = tr.xpath(MATCH_DATE_TIME_XPATH % counter).re('datet\st(\d+)')
                    participants = tr.xpath(PARTICIPANTS_XPATH % counter).extract()
                    participants_link = tr.xpath(PARTICIPANTS_LINK_XPATH % counter).extract_first()
                    score = tr.xpath(SCORE_XPATH % counter).extract()
                    var_one = tr.xpath(VAR_ONE_XPATH % counter).extract()
                    var_x = tr.xpath(VAR_X_XPATH % counter).extract()
                    var_two = tr.xpath(VAR_TWO_XPATH % counter).extract()
                    var_b = tr.xpath(VAR_B_XPATH % counter).extract()

                    # cleaning the data
                    _date = ""
                    _time = ""
                    if match_date_time:
                        date_time = datetime.datetime.fromtimestamp(int(match_date_time[0]))
                        _date = date_time.strftime("%d %b %Y ")
                        _time = date_time.strftime("%H:%M")
                        if (date_time.date() < datetime.date(2017,8,20)):
                            return;
                    participants = ' '.join(' '.join(participants).strip().split())
                    team_a, team_b = participants.split(' - ')
                    match_link = urljoin(self.base_url, participants_link) if participants_link else ""
                    score = ' '.join(' '.join(score).split())
                    team_a_score, team_b_score = score.split(':') if ":" in score else (score, score)
                    if not team_a_score.isdigit():
                        team_won = ""
                    else:
                        if team_a_score > team_b_score:
                            team_won = team_a
                        elif team_a_score < team_b_score:
                            team_won = team_b
                        else:
                            team_won = ""
                    var_one = self.decrypt(var_one[0]) if var_one else ""
                    var_x = self.decrypt(var_x[0]) if var_x else ""
                    var_two = self.decrypt(var_two[0]) if var_two else ""
                    var_b = var_b[0] if var_b else ""
                    if header and _date:
                        if _date in header:
                            header = header.strip()
                        else:
                            header = _date + ' '.join(''.join(header).split())
                    else:
                        header = _date + ' '.join(''.join(header).split()) if header else _date

                    # item initialization
                    # item = OddsportalItem()
                    item = {}
                    item['Game'] = "Soccer"
                    item['Country'] = country
                    item['League'] = tournament_name
                    item['League Year'] = tournament_year
                    item['Game Date'] = _date
                    item['Game Time'] = _time
                    item['Team A'] = team_a.strip()
                    item['Team B'] = team_b.strip()
                    item['Team A Score'] = team_a_score.strip()
                    item['Team B Score'] = team_b_score.strip().rstrip('pen.')
                    item['Team Won'] = team_won
                    item['1'] = var_one
                    item['X'] = var_x
                    item['2'] = var_two
                    item['Bs'] = var_b
                    item['url'] = match_link
                    next_meta = {"item": item}
                    # yield item
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
        date_flag = True
        meta = response.meta
        referer = meta.get("refer")
        param_id = meta.get("param_id")
        country = meta.get("country")
        header = {"Referer": referer}
        tournament_year = meta.get("tournament_year")
        tournament_name = meta.get("tournament_name")
        # meta_data = {"refer": referer, "param_id": param_id, "country": country, "tournament_name": meta.get("tournament_name"), "tournament_year": meta.get("tournament_year")}
        content = re.findall(r'(\{.*\})\)\;', response.body_as_unicode(), re.M)
        if content:
            content = json.loads(content[0])
            sel = Selector(text=content.get('d', {}).get("html"))
            date_in_url = re.findall(r'(\d{4}\-?\d{0,4})\/results\/$', referer)
            if not date_in_url:
                tournament_year = meta.get("tournament_year")
                tournament_name = meta.get("tournament_name")
            else:
                date_flag = False
                bread_crumbs = sel.xpath('//tr[@class="dark center"][1]/th/a/text()').extract()
                league_year = bread_crumbs[-1].rsplit(' ', 1)
                tournament_name = league_year[0]
                tournament_year = league_year[1]

            # XPATHS
            trs = sel.xpath('//tr[@class="center nob-border"]')
            PARTICIPANTS_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="name table-participant"]/a//text()'
            PARTICIPANTS_LINK_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="name table-participant"]/a/@href'
            MATCH_DATE_TIME_XPATH = './self::tr/following-sibling::tr[%s]/td[contains(@class, "datet")]/@class'
            SCORE_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="center bold table-odds table-score"]//text()'
            VAR_ONE_XPATH = './self::tr/following-sibling::tr[%s]/td[4]/@xodd'
            VAR_X_XPATH = './self::tr/following-sibling::tr[%s]/td[5]/@xodd'
            VAR_TWO_XPATH = './self::tr/following-sibling::tr[%s]/td[6]/@xodd'
            VAR_B_XPATH = './self::tr/following-sibling::tr[%s]/td[@class="center info-value"]/text()'
            for index, tr in enumerate(trs):
                counter = 0
                header = tr.xpath('./self::tr/th[@class="first2 tl"]/text()').extract_first()
                if not date_flag:
                    th_data = tr.xpath('./self::tr/preceding-sibling::tr[@class="dark center"][1]/th/a/text()').extract()
                    info = th_data[-1].rsplit(' ', 1)
                    tournament_year = info[1].strip()
                    tournament_name = info[0].strip()
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
                    match_date_time = tr.xpath(MATCH_DATE_TIME_XPATH % counter).re('datet\st(\d+)')
                    participants = tr.xpath(PARTICIPANTS_XPATH % counter).extract()
                    participants_link = tr.xpath(PARTICIPANTS_LINK_XPATH % counter).extract_first()
                    score = tr.xpath(SCORE_XPATH % counter).extract()
                    var_one = tr.xpath(VAR_ONE_XPATH % counter).extract()
                    var_x = tr.xpath(VAR_X_XPATH % counter).extract()
                    var_two = tr.xpath(VAR_TWO_XPATH % counter).extract()
                    var_b = tr.xpath(VAR_B_XPATH % counter).extract()

                    # cleaning the data
                    _date = ""
                    _time = ""
                    if match_date_time:
                        date_time = datetime.datetime.fromtimestamp(int(match_date_time[0]))
                        _date = date_time.strftime("%d %b %Y ")
                        _time = date_time.strftime("%H:%M")
                        if (date_time.date() < datetime.date(2017,8,20)):
                            return;
                    participants = ' '.join(' '.join(participants).strip().split())
                    team_a, team_b = participants.split(' - ')
                    match_link = urljoin(self.base_url, participants_link) if participants_link else ""
                    score = ' '.join(' '.join(score).split())
                    team_a_score, team_b_score = score.split(':') if ":" in score else (score, score)
                    if not team_a_score.isdigit():
                        team_won = ""
                    else:
                        if team_a_score > team_b_score:
                            team_won = team_a
                        elif team_a_score < team_b_score:
                            team_won = team_b
                        else:
                            team_won = ""
                    var_one = self.decrypt(var_one[0]) if var_one else ""
                    var_x = self.decrypt(var_x[0]) if var_x else ""
                    var_two = self.decrypt(var_two[0]) if var_two else ""
                    var_b = var_b[0] if var_b else ""
                    if header and _date:
                        if _date in header:
                            header = header.strip()
                        else:
                            header = _date + ' '.join(''.join(header).split())
                    else:
                        header = _date + ' '.join(''.join(header).split()) if header else _date

                    # item initialization
                    # item = OddsportalItem()
                    item = {}
                    item['Game'] = "Soccer"
                    item['Country'] = country
                    item['League'] = tournament_name
                    item['League Year'] = tournament_year
                    item['Game Date'] = _date
                    item['Game Time'] = _time
                    item['Team A'] = team_a.strip()
                    item['Team B'] = team_b.strip()
                    item['Team A Score'] = team_a_score.strip()
                    item['Team B Score'] = team_b_score.strip().rstrip('pen.')
                    item['Team Won'] = team_won
                    item['1'] = var_one
                    item['X'] = var_x
                    item['2'] = var_two
                    item['Bs'] = var_b
                    item['url'] = match_link
                    next_meta = {"item": item}
                #    yield item
                    yield Request(url=match_link, callback=self.parse_score, meta=next_meta) 

    def parse_score(self, response):
        url = 'http://fb.oddsportal.com/feed/match/{versionId}-{sportId}-{id}-1-2-{xhash}.dat?_='
        meta = response.meta
        header = {"Referer": response.url}
        item = meta.get("item")
        result = response.xpath('//div[@id="event-status"]/p[@class="result"]/text()').extract()
        param_dict = self.get_param_id(response, "PageEvent")
        if param_dict:
            param_dict['xhash'] = urllib.parse.unquote(param_dict.get("xhash"))
            if result:
                team_a_1st_half, team_b_1st_half = 0, 0
                team_a_2nd_half, team_b_2nd_half = 0, 0
                scores = result[0].strip().strip(')').strip('(').strip().split(",")
                if len(scores) == 2:
                    team_a_1st_half, team_b_1st_half = scores[0].split(':')
                    team_a_2nd_half, team_b_2nd_half = scores[1].split(':')
                item['team_a_1st_half'] = team_a_1st_half
                item['team_b_1st_half'] = team_b_1st_half
                item['team_a_2nd_half'] = team_a_2nd_half
                item['team_b_2nd_half'] = team_b_2nd_half
                meta_data = {"item": item}
                link = url.format(**param_dict)
                link = link + repr(time.time()).replace('.', '')[:-3]
                yield Request(url=link, callback=self.parse_final, meta=meta_data, headers=header)

    def parse_final(self, response):
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
                pinnacle_x = ""
                pinnacle_2 = ""
                payout = ""
                if odds:
                    if isinstance(odds, dict):
                        pinnacle_1 = odds.get("0")
                        pinnacle_x = odds.get("1")
                        pinnacle_2 = odds.get("2")
                    else:
                        pinnacle_1, pinnacle_x, pinnacle_2 = odds
                    data_list = [pinnacle_1, pinnacle_x, pinnacle_2]
                    payout = self.calculate_payout(data_list)
                item["pinnacle_1"] = pinnacle_1
                item["pinnacle_x"] = pinnacle_x
                item["pinnacle_2"] = pinnacle_2
                item['pinnacle_payout'] = payout
                item['date_created']= ""
                item['created_by']= ""
                item['date_modified']= ""
                item['modified_by']= ""
                yield item
            except Exception as e:
                pass
