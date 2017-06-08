import urllib2
from collections import deque

import scrapy
import re
import cgi
import os
import HTMLParser


class EdiSpider(scrapy.Spider):
    name = "edi"
    url = 'https://portal.edirepository.org/nis/browseServlet?searchValue=genomics'
    directory = url.split("=")[-1]
    if not os.path.exists(directory):
        os.makedirs(directory)

    data_package = ""
    pagination_urls = []

    def start_requests(self):
        # self.pagination_urls.append(self.url)
        yield scrapy.Request(self.url, callback=self.parse)
        yield scrapy.Request(self.url, callback=self.prepare_url_list)

    def prepare_url_list(self, response):
        temp_url = response.xpath('//a[contains(@href, "simpleSearch")]').extract()
        regex = re.compile(".*(&gt;)")
        intermediate_url = [matcher.group(0) for url in temp_url for matcher in [regex.search(url)] if matcher]
        if intermediate_url:
            intermediate_url_href = re.search("simpleSearch(.*)(asc|desc)", intermediate_url[0]).group(0)
            intermediate_url_href = HTMLParser.HTMLParser().unescape(intermediate_url_href)
            next_page = response.urljoin(intermediate_url_href)
            if next_page is not None:
                # self.pagination_urls.append(next_page)
                yield scrapy.Request(next_page, callback=self.parse)
                yield scrapy.Request(next_page, callback=self.prepare_url_list)
        # else:
        #     for page in self.pagination_urls:
        #         # print page
        #         yield scrapy.Request(page, callback=self.parse)
            
    def parse(self, response):
        topics_overview_page = set(response.css('td.nis a::attr(href)').extract())
        print "response.url:", response.url
        if topics_overview_page:
            # count = 1
            for data_lake_url in topics_overview_page:
                # if count > 2:
                #     break
                data_lake_overview_page = response.urljoin(data_lake_url)
                print "new_url:", data_lake_overview_page
                EdiSpider.package_name = data_lake_overview_page.split("=")[1]
                EdiSpider.package_name = os.path.join(EdiSpider.directory + os.sep, EdiSpider.package_name)
                if not os.path.exists(EdiSpider.package_name):
                    os.makedirs(EdiSpider.package_name)
                # count += 1
                yield scrapy.Request(data_lake_overview_page, callback=self.get_data_links_from_data_lake)

    def get_data_links_from_data_lake(self, response):
        print "inside get_data"
        all_links = response.css("a.searchsubcat::attr(href)").extract()
        regex = re.compile(".*(\\bdataviewer).*")
        data_links = [data_link.group(0) for link in all_links for data_link in [regex.search(link)] if data_link]
        for link in data_links:
            yield scrapy.Request(response.urljoin(link), callback=self.download_data_from_data_lake)

    def download_data_from_data_lake(self, response):
        http_response = urllib2.urlopen(response.url)
        _, params = cgi.parse_header(http_response.headers.get('Content-Disposition',''))
        filename = params['filename']
        EdiSpider.package_name = re.search("packageid=(.*)&", response.url).group(1)
        EdiSpider.package_name = os.path.join(EdiSpider.directory + os.sep, EdiSpider.package_name)
        with open(os.path.join(EdiSpider.package_name, filename), 'wb') as file:
            self.logger.info("Saving file %s/%s", EdiSpider.package_name, filename)
            file.write(response.body)

from scrapy import cmdline
cmdline.execute("scrapy crawl edi".split())
