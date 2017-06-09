import urllib2
import scrapy
import re
import cgi
import os
import HTMLParser
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


class Links(scrapy.Item):
    main_url = scrapy.Field()


class EdiSpider(scrapy.Spider):
    name = "data"
    start_urls = ['https://portal.edirepository.org/nis/browseServlet?searchValue=genomics']
    directory = start_urls[0].split("=")[-1]
    if not os.path.exists(directory):
        os.makedirs(directory)

    def parse(self, response):
        print "response.url:", response.url
        item = Links()
        item['main_url'] = set()
        item['main_url'].add(response.url)
        request = scrapy.Request(response.url, callback=self.get_page_links)
        request.meta['item'] = item
        yield request

    def get_page_links(self, response):
        temp_url = response.xpath('//a[contains(@href, "simpleSearch")]').extract()
        regex = re.compile(".*(&gt;)")
        intermediate_url = [matcher.group(0) for url in temp_url for matcher in [regex.search(url)] if matcher]
        item = response.meta['item']
        if intermediate_url:
            intermediate_url_href = re.search("simpleSearch(.*)(asc|desc)", intermediate_url[0]).group(0)
            intermediate_url_href = HTMLParser.HTMLParser().unescape(intermediate_url_href)
            next_page = response.urljoin(intermediate_url_href)
            # yield scrapy.Request(next_page, callback=self.get_table_links)
            item['main_url'].add(next_page)
            request = scrapy.Request(next_page, callback=self.get_page_links)
            request.meta['item'] = item
            yield request
        else:
            for page in item['main_url']:
                yield scrapy.Request(page, callback=self.get_table_links, dont_filter=True)

    def get_table_links(self, response):
        for data_lake_url in set(response.css('td.nis a::attr(href)').extract()):
            data_lake_overview_page = response.urljoin(data_lake_url)
            print "new_url:", data_lake_overview_page
            EdiSpider.package_name = data_lake_overview_page.split("=")[1]
            EdiSpider.package_name = os.path.join(EdiSpider.directory + os.sep, EdiSpider.package_name)
            if not os.path.exists(EdiSpider.package_name):
                os.makedirs(EdiSpider.package_name)
            yield scrapy.Request(data_lake_overview_page, callback=self.get_data_links)

    def get_data_links(self, response):
        print "inside get_data link"
        all_links = response.css("a.searchsubcat::attr(href)").extract()
        regex = re.compile(".*(\\bdataviewer).*")
        data_links = [data_link.group(0) for link in all_links for data_link in [regex.search(link)] if data_link]
        for link in data_links:
            yield scrapy.Request(response.urljoin(link), callback=self.download_data_files, errback=self.errback)

    def download_data_files(self, response):
        http_response = urllib2.urlopen(response.url)
        _, params = cgi.parse_header(http_response.headers.get('Content-Disposition', ''))
        filename = params['filename']
        EdiSpider.package_name = re.search("packageid=(.*)&", response.url).group(1)
        EdiSpider.package_name = os.path.join(EdiSpider.directory + os.sep, EdiSpider.package_name)
        with open(os.path.join(EdiSpider.package_name, filename), 'wb') as file:
            self.logger.info("Saving file %s/%s", EdiSpider.package_name, filename)
            file.write(response.body)

    def errback(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:
        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

        response = failure.value.response
        yield scrapy.Request(response.url, dont_filter=True, callback=self.download_data_files)


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
process.crawl(EdiSpider)
process.start()
