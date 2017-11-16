import scrapy
import json
from scrapy.selector import Selector
from mcrg.items import CoversLineHistoryItem
import logging
from scrapy.utils.log import configure_logging
from datetime import datetime, timedelta

# sports = ["AM_FOOTBALL","MLB","AUSSIE","BASKETBALL","ESPORTS","HOCKEY","NBA","NHL","RUGBY","SOCCER","TENNIS","VOLLEYBALL","NCAAB","KHL","NCAAF","NFL","WNBA"]
# trends = ["92","2084","286","1957","283","285","2040","2027","179","1670","136","1244","19","2113","1245","67","323","1738","303","2115","2074","1961","1429","390","1868","1948","1515","1753","2437","1812","216","2435","1932","1787","2431","531","1956","183","1210","1946","2075","60","2438","1500","1869","49","1897","50","489","1424","404","1401","1514","1512","1601","1959","1323","2114","2447","1322","302","1943","1166","2076","1928","1826","98","1960","1264","294","1454","2436","1672","1758","1540","1453","299","1664","1321","1541","1167","1671","371","1752","1543","1544","1931","2374","1542","345","1406","55","1327","1248","359","1326","1177","1320","1328","304","1324","1545","2108","1200","77","1914","613","1292","376","614","2044","2244","1862","846","1830","1112","812","1225","1206","1913","1318","2450","1703","1325","369","1441","1460","53","1754","1","20","258","1829","1513","1461","1911","292","836","1421","1747","301","1246","33","358","3","181","1265","1501","1815","56","93","21","472","1312","180","73","351","2","58","51","321","32","22","86","52","57","54","68","357","2026","94","31","97","355"]

configure_logging(install_root_handler=False)
logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO)

class CoversSpider(scrapy.Spider):
    id = 5
    name = 'zcode_sports_trader_spider'
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

    # fetch all the top system url       
    def parse_sportstrader(self, response):
        self.logger.info("Just arrived at %s", response.url)

        headers = {
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.8",        
        "Connection": "keep-alive",
        "Content-Length": "291",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "language=en; referrer=122.176.20.28%3A%3Azcodesystem.com%252Fvipclub%252Flogin; zc_vippicks_cur_sport=SOCCER; __utma=187974435.1724478810.1486967851.1487931528.1489057675.3; __utmz=187974435.1487927784.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); ZC_LANG=en; mp_6b62034815c12d29db73e6f6fcbd92c2_mixpanel=%7B%22distinct_id%22%3A%20%2215a3630c69d34-0c0c7e8a8bdfc1-57e1b3c-100200-15a3630c69e23%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D; _ga=GA1.2.1724478810.1486967851; sessid2=sessid20170213063810983; PHPSESSID=4be6519d3035c261b584f24d9783579f; show_club_menu=0; show_picks_menu=1; ST_APP_MODE=Hbd6eooh",
        "Host": "zcodesystem.com",
        "Origin": "http://zcodesystem.com",
        "Referer": "http://zcodesystem.com/sports_trader/start.php",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        }
        data = {"sports":["NHL"],"typecalc":0,"unitsize":100,
        # "trends":["1738","1429","1787","531","1500","1424","1166","1454","1758","1453","1664","1167","1248","1200","67","442","1914","2044","1862","1913","139","443","131","132","441","134","138","140","133","395","137"],
        "trends": ["1738","1429","1664","1787","1758","531","1424","1500","1166","1248","1454","1453","1200","1167"],
        "get_date_data":"true","asort_mode":0,"psort_mode":0}
        url = "http://zcodesystem.com/sports_trader/get_active_signals.php"
        yield scrapy.Request(
                    url,
                    self.parse_review,
                    method= 'POST',
                    headers= headers,
                    body= json.dumps(data)
            )
        

    def parse_review(self, response):
        print("============Response Body====================")
        jsonresponse = json.loads(response.body_as_unicode())
        next_games = jsonresponse["next_games"]
        past_html = jsonresponse["past_html"]
        # print(jsonresponse)
        print(jsonresponse)
        