# -*- coding: utf-8 -*-
import csv
import time

import scrapy
from selenium import webdriver
from scrapy.selector import Selector

from mcrg.items import ZcodeItem


class ZCodeSpider(scrapy.Spider):
    id = 8
    name = 'zcode_past_picks_spider'
    allowed_domains = ['zcodesystem.com']
    start_urls = ['http://zcodesystem.com/']

    def clean_data(self, sel, x_path, split=False):
        data = sel.xpath(x_path).extract()
        if split:
            data_value = ' '.join(' '.join(data).split())
        else:
            data_value = data[0].strip() if data else ""
        return data_value


    def fetch_data(self, sel):
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        SYSTEM_NAME = './td/div[@class="left_shift system_name"]/text()'
        GROUP_TYPE = './td[@class="group_col sgame"]/div[@class="left_shift" and position()=3]/text()'
        TEAM_NAME = './td[@class="group_col sgame"]/div/div[@class="teamName"]/div[@class="Text"]/text()'
        ROTATIONAL_NUMBER = './td[@class="group_col sgame"]/div/div[@class="teamName"]/div[@class="Text"]/div[@class="rot_num"]/text()'
        BET_ON_DATE = './td[@class="beton_col"]/input[@name="gdate"]/@value'
        BET_ON_GROUP_ID = './td[@class="beton_col"]/input[@name="group_id"]/@value'
        BET_ON_SIGNAL_ID = './td[@class="beton_col"]/input[@name="signal_id"]/@value'
        BET_ON = './td[@class="beton_col"]/text()'
        ODD = './td[@class="odd"]/text()'
        UNIT = './td[@class="unit"]/text()'
        BET = './td[@class="bet"]/text()'
        GAME_DATA = './td[@class="game_col"]/text()'
        GAME_TYPE = './td[@class="game_col"]/strong/text()'

        trs = sel.xpath('//div[@id="ActiveTab"]/div/table/tbody/tr')
        print(len(trs))
        data_list = []
        for tr in trs:
            item = {}
            system_name = self.clean_data(tr, SYSTEM_NAME)
            group_type = self.clean_data(tr, GROUP_TYPE)
            team_name = self.clean_data(tr, TEAM_NAME)
            rotational_number = self.clean_data(tr, ROTATIONAL_NUMBER)
            bet_on_date = self.clean_data(tr, BET_ON_DATE)
            bet_on_group_id = self.clean_data(tr, BET_ON_GROUP_ID)
            bet_on_signal_id = self.clean_data(tr, BET_ON_SIGNAL_ID)
            bet_on = self.clean_data(tr, BET_ON, True)
            odd = self.clean_data(tr, ODD)
            unit = self.clean_data(tr, UNIT)
            bet = self.clean_data(tr, BET)
            game_data = self.clean_data(tr, GAME_DATA)
            game_type = self.clean_data(tr, GAME_TYPE)

            item["system_name"] = system_name
            item["group_type"] = group_type
            item["team_name"] = team_name
            item["rotational_number"] = rotational_number
            item["bet_on_date"] = bet_on_date
            item["bet_on_group_id"] = bet_on_group_id
            item["bet_on_signal_id"] = bet_on_signal_id
            item["bet_on"] = bet_on
            item["odd"] = odd
            item["unit"] = unit
            item["bet"] = bet
            item["game_data"] = game_data
            item["game_type"] = game_type
            if team_name:
                data_list.append(item)
        return data_list


    def parse(self, response):
        driver = webdriver.Chrome(r"C:\Users\Administrator\Downloads\chromedriver_win32\chromedriver.exe")

        driver.get("https://zcodesystem.com/vipclub/do_login.php")

        input_username = "grao@castlerockresearch.com"
        input_password = "Salmana29$"

        username = driver.find_element_by_id("emailaddress")
        password = driver.find_element_by_id("password")
        submit = driver.find_element_by_name('submit')

        username.send_keys(input_username)
        password.send_keys(input_password)
        submit.click()

        driver.get("http://zcodesystem.com/sports_trader/start.php")
        system_selector = driver.find_elements_by_xpath('//div[@id="SystemSelector"]/div/div/label')
        trends_selector = driver.find_elements_by_xpath('//div[@id="TrendSelector"]/div/div/label')
        if system_selector:
            system_selector[0].click()
            time.sleep(5)
            if trends_selector:
                trends_selector[0].click()
                time.sleep(15)
                sel = Selector(text=driver.page_source)
                data_list = self.fetch_data(sel)
                for info in data_list:
                    item = ZcodeItem(**info)
                    yield item
                driver.close()