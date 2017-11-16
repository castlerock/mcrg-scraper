import scrapy
import boto
import os, sys
import boto.s3.connection
conn = boto.connect_s3(
        aws_access_key_id = 'AKIAJU25YD4BJX4CQJ3A',
        aws_secret_access_key = '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',
        host = 's3.ca-central-1.amazonaws.com',
        )
b = conn.get_bucket('devmcrgzcodehisthtml', validate=False) 
from boto.s3.key import Key
from mcrg.items import ZCodeSystemHTMLPathItem

import logging
from scrapy.utils.log import configure_logging

configure_logging(install_root_handler=False)
logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO
    )

class LoginSpider(scrapy.Spider):
    id = 1
    name = 'zcodehtmltos3_spider'
    download_delay = 0.5	
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
            request = scrapy.Request("http://zcodesystem.com/topsystems.php", callback=self.parse_topsystems)
            yield request

    # fetch all the top system url       
    def parse_topsystems(self, response):
        logging.info('Just arrived at url parse_topsystems %s' % response.url)
        links = response.xpath("//span[contains(@class,'system_name')]/ancestor::a/@href").extract()
        print("Count is here %",len(links))		
        for index, link in enumerate(links):
            #print('Found link %s', link)
            logging.info('Just arrived at link %s' % link)
            if link != '/vipclub/esportsystem.php':
                request = scrapy.Request("http://zcodesystem.com"+ link,callback=self.download_pagehtml,errback=self.errback_httpbin)
                yield request
        return
		
    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))
		
    # download the source code and save html
    def download_pagehtml(self, response):
        logging.info('Just arrived at download_pagehtml %s' % response.url)
        filename = response.url.split("/")[-1]
        filename = filename.replace('php','html')   
        item = ZCodeSystemHTMLPathItem()
        item['page_url'] = response.url
        item['html_file_url'] = 'https://s3.ca-central-1.amazonaws.com/devmcrgzcodehisthtml/' + filename
        
        def percent_cb(complete, total):
            sys.stdout.write('.')
            sys.stdout.flush()

        k = Key(b)
        k.key = filename
        k.set_contents_from_string(response.body, cb=percent_cb, num_cb=10)
        k.make_public()
        yield item 
        print("===========================")
        self.log('Saved file %s' % filename)
        print("===========================")





