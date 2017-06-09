import urllib2
from collections import deque

import scrapy
import re
import cgi
import os
import HTMLParser



class EdiSpider(scrapy.Spider):
    name = "edi"
    url = 'https://portal.edirepository.org/nis/browseServlet?searchValue=water+balance'
    start_urls = []
    start_urls.append(url)
    pages = ['https://portal.edirepository.org/nis/simpleSearch?start=%s&rows=10&sort=score,desc' % page for page in xrange(10,30,10)]
    for page in pages:
        start_urls.append(page)
    directory = url.split("=")[-1]
    if not os.path.exists(directory):
        os.makedirs(directory)

    data_package = ""
    pagination_urls = []

    # def start_requests(self):
    #     # self.pagination_urls.append(self.url)
    #     # yield scrapy.Request(self.url, callback=self.prepare_url_list)
    #     # yield scrapy.Request(self.url, callback=self.parse,headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3"})
    #
    #     yield scrapy.Request(EdiSpider.start_urls[0], callback=self.prepare_url_list)
    #
    # def prepare_url_list(self, response):
    #     temp_url = response.xpath('//a[contains(@href, "simpleSearch")]').extract()
    #     regex = re.compile(".*(&gt;)")
    #     intermediate_url = [matcher.group(0) for url in temp_url for matcher in [regex.search(url)] if matcher]
    #     if intermediate_url:
    #         intermediate_url_href = re.search("simpleSearch(.*)(asc|desc)", intermediate_url[0]).group(0)
    #         intermediate_url_href = HTMLParser.HTMLParser().unescape(intermediate_url_href)
    #         next_page = response.urljoin(intermediate_url_href)
    #         if next_page is not None:
    #             # self.pagination_urls.append(next_page)
    #             # yield scrapy.Request(next_page, callback=self.parse,headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3"})
    #             # yield scrapy.Request(next_page, callback=self.prepare_url_list,headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3"})
    #             # EdiSpider.start_urls.append(next_page)
    def parse(self, response):
        topics_overview_page = set(response.css('td.nis a::attr(href)').extract())
        print "response.url:", response.url
        if topics_overview_page:
            count = 1
            for data_lake_url in topics_overview_page:
                # if count > 2:
                #     break
                data_lake_overview_page = response.urljoin(data_lake_url)
                print "new_url:", data_lake_overview_page
                EdiSpider.package_name = data_lake_overview_page.split("=")[1]
                EdiSpider.package_name = os.path.join(EdiSpider.directory + os.sep, EdiSpider.package_name)
                if not os.path.exists(EdiSpider.package_name):
                    os.makedirs(EdiSpider.package_name)
                count += 1
                yield scrapy.Request(data_lake_overview_page, callback=self.get_data_links_from_data_lake, headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3"})

    def get_data_links_from_data_lake(self, response):
        print "inside get_data"
        all_links = response.css("a.searchsubcat::attr(href)").extract()
        regex = re.compile(".*(\\bdataviewer).*")
        data_links = [data_link.group(0) for link in all_links for data_link in [regex.search(link)] if data_link]
        for link in data_links:
            yield scrapy.Request(response.urljoin(link), callback=self.download_data_from_data_lake,headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3"})

    def download_data_from_data_lake(self, response):
        http_response = urllib2.urlopen(response.url)
        _, params = cgi.parse_header(http_response.headers.get('Content-Disposition',''))
        filename = params['filename']
        EdiSpider.package_name = re.search("packageid=(.*)&", response.url).group(1)
        EdiSpider.package_name = os.path.join(EdiSpider.directory + os.sep, EdiSpider.package_name)
        with open(os.path.join(EdiSpider.package_name, filename), 'wb') as file:
            self.logger.info("Saving file %s/%s", EdiSpider.package_name, filename)
            file.write(response.body)
    def errback(self, url):
        yield scrapy.Request(url, dont_filter=True, callback=self.parse, errback=lambda x: self.parse(x, url),headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3"})

from scrapy.crawler import CrawlerProcess
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
process.crawl(EdiSpider)
process.start()