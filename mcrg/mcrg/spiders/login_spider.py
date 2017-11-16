import scrapy
import boto
import os, sys
import boto.s3.connection
conn = boto.connect_s3(
        aws_access_key_id = 'AKIAJU25YD4BJX4CQJ3A',
        aws_secret_access_key = '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',
        host = 's3.ca-central-1.amazonaws.com',
        #is_secure=False,               # uncomment if you are not using ssl
        #calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
b = conn.get_bucket('devmcrgzcodehisthtml', validate=False) 
from boto.s3.key import Key
from mcrg.items import SpiderItem

class LoginSpider(scrapy.Spider):
    id = 1
    name = 'zcodehistorylogin_spider'
    start_urls = ['http://zcodesystem.com/vipclub/login.php']

    def parse(self, response):
        return scrapy.FormRequest.from_response(
            response,
            formdata={'emailaddress': 'grao@castlerockresearch.com', 'password': 'salmana'},
            callback=self.after_login
        )

    def after_login(self, response):
        # check login succeed before going on
        if b"Password is incorrect" in response.body:
            self.logger.error("Login failed")
            return

        # continue scraping with authenticated session...
        if b"Welcome to the VIP Club" in response.body:
            request = scrapy.Request("http://zcodesystem.com/topsystems.php", callback=self.parse_topsystems)
            yield request

    # fetch all the top system url       
    def parse_topsystems(self, response):
        self.logger.info("Just arrived at %s", response.url)
        links = response.xpath("//span[contains(@class,'system_name')]/ancestor::a/@href").extract()
        for index, link in enumerate(links):
            #print('Found link %s', link)
            request = scrapy.Request("http://zcodesystem.com"+ link,callback=self.download_pagehtml)
            yield request
        return

    # download the source code and save html
    def download_pagehtml(self, response):
        #self.logger.info("Downloading page html at %s", response.url)    
                
        filename = response.url.split("/")[-1]
        filename = filename.replace('php','html')   

        item = SpiderItem()
        item['name'] = 'zcodehistorylogin_spider'
        yield item 
        
        # def percent_cb(complete, total):
        #     sys.stdout.write('.')
        #     sys.stdout.flush()

        # k = Key(b)
        # k.key = filename
        # k.set_contents_from_string(response.body, cb=percent_cb, num_cb=10)

        #https://s3.ca-central-1.amazonaws.com/devmcrgzcodehisthtml/weppa_andersson_system.html
        # with open(filename, 'wb') as f:
        #     # mp.upload_part_from_file(f)
        #     #s3.send_file(f)
        #     f.write(response.body)
        self.log('Saved file %s' % filename)





