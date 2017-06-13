import scrapy
import re
import os
import HTMLParser
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


class Links(scrapy.Item):
    main_url = scrapy.Field()
    pagination_url_pattern = scrapy.Field()
    pagination_urls = scrapy.Field()


class ListCrawler(scrapy.Spider):
    name = "list"
    start_urls = ['https://portal.edirepository.org/nis/browseServlet?searchValue=genomics']
    directory = start_urls[0].split("=")[-1]
    if not os.path.exists(directory):
        os.makedirs(directory)

    def parse(self, response):
        pagination_url_pattern = raw_input("Enter the pagination URL pattern: ")
        item = Links()
        item['main_url'] = set()
        item['main_url'].add(response.url)
        item['pagination_url_pattern'] = pagination_url_pattern
        item['pagination_urls'] = []
        request = scrapy.Request(response.url, callback=self.get_page_links)
        request.meta['item'] = item
        yield request

    def write_to_file(self, file_path, content):
        file_object = open(file_path, 'w+')
        for item in content:
            self.logger.info("Writing URL %s to %s", item, file_object.name)
            file_object.write(item + "\n")
        file_object.close()

    def get_page_links(self, response):
        item = response.meta['item']
        temp_url = response.xpath('//a[contains(@href,' + "\"" + item['pagination_url_pattern'] + '\")]').extract()
        regex = re.compile(".*(&gt;|next|Next)")
        intermediate_url = [matcher.group(0) for url in temp_url for matcher in [regex.search(url)] if matcher]

        if intermediate_url:
            intermediate_url_href = re.search("href=\"(.*)\"", intermediate_url[0]).group(1)
            intermediate_url_href = HTMLParser.HTMLParser().unescape(intermediate_url_href)
            next_page = response.urljoin(intermediate_url_href)
            item['pagination_urls'].append(next_page)
            request = scrapy.Request(next_page, callback=self.get_page_links)
            request.meta['item'] = item
            yield request
        else:
            pagination_links_dir = ListCrawler.directory + os.sep + "paginations"
            if not os.path.exists(pagination_links_dir):
                os.makedirs(pagination_links_dir)
            filename = "pagination_urls.txt"
            file_path = os.path.join(pagination_links_dir, filename)
            self.write_to_file(file_path, item['pagination_urls'])

    def errback(self, failure):
        
        """
        Handles any exception that occurs while crawling and reissues a request to the server
        for the URL which failed.
        :param failure: Error details
        """
        # Logs all failures
        self.logger.error(repr(failure))

        # Checking the type of failure and handling it accordingly
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # This is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

        # Reissuing a request
        response = failure.value.response
        yield scrapy.Request(response.url, dont_filter=True, callback=self.download_data_files)


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
process.crawl(ListCrawler)
process.start()
