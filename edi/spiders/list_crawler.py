import HTMLParser
import os
import re

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.project import get_project_settings
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


class Links(scrapy.Item):
    """
        This class defines the fields for storing the metadata in between requests

    """
    main_url = scrapy.Field()
    pagination_url_pattern = scrapy.Field()
    pagination_urls = scrapy.Field()


class ListCrawler(scrapy.Spider):
    # Spider's name
    scrapy.Spider.name = "list"

    pagination_file_name = "pagination_urls.txt"

    # Getting start page's URL from user
    start_urls = []
    input_url = raw_input("Enter home URL: ")
    start_urls.append(input_url)

    # Creating a directory for the domain
    directory = start_urls[0].split("=")[-1]

    directory_path = raw_input("Enter a path to save the downloaded files: ")
    directory_path = directory_path + os.sep + directory
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Writing the path to a directory to be read by the Path Crawler
    file_object = open(os.path.join(os.getcwd(), 'pagination_url_path.txt'), 'w+')
    file_object.write(directory_path + os.sep + pagination_file_name)
    file_object.close()

    def parse(self, response):
        """
        The starting point of the crawler. Gets the URL pattern for the pagination links from
        the user and issues a request to collect them
        :param response: The response packet received after making a request to the server

        """
        pagination_url_pattern = raw_input("Enter the pagination URL pattern (E.g. simpleSearch?): ")
        item = Links()

        # Setting metadata to be used in the subsequent request
        item['main_url'] = set()
        item['main_url'].add(response.url)
        item['pagination_url_pattern'] = pagination_url_pattern
        item['pagination_urls'] = [response.url]

        # Making a request to collect all pagination links
        request = scrapy.Request(response.url, callback=self.get_page_links)

        # Encapsulating the metadata in the request
        request.meta['item'] = item
        yield request

    def write_to_file(self, file_path, content):
        """
        :param file_path: The file to which the pagination URLs will be written
        :param content: List of all the pagination URLs with the home URL

        """
        file_object = open(file_path, 'w+')
        for item in content:
            self.logger.info("Writing URL %s to %s", item, file_object.name)
            file_object.write(item + '\n')
        file_object.close()

    def get_page_links(self, response):
        """

        :param response: he response packet received after making a request to the server

        """

        # Unpacking the metadata from the response
        item = response.meta['item']

        # Extracting all the pagination URL matching the URL pattern provided by the user
        temp_url = response.xpath('//a[contains(@href,' + "\"" + item['pagination_url_pattern'] + '\")]').extract()

        # Regex to match the next page URL (denoted by '>', 'next' or 'Next')
        regex = re.compile(".*(&gt;|next|Next)")
        intermediate_url = [matcher.group(0) for url in temp_url for matcher in [regex.search(url)] if matcher]

        if intermediate_url:
            intermediate_url_href = re.search("href=\"(.*)\"", intermediate_url[0]).group(1)
            intermediate_url_href = HTMLParser.HTMLParser().unescape(intermediate_url_href)

            # Forming the complete pagination URL by joining it with the parent URL
            next_page = response.urljoin(intermediate_url_href)

            # Stroing the collected pagination links in metadata
            item['pagination_urls'].append(next_page)

            # Recursive request to collect all pagination URLs
            request = scrapy.Request(next_page, callback=self.get_page_links)
            request.meta['item'] = item
            yield request
        else:
            file_path = os.path.join(ListCrawler.directory_path, self.pagination_file_name)
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
            self.logger.error('HttpError on %s', failure)

        elif failure.check(DNSLookupError):
            # This is the original request
            self.logger.error('DNSLookupError on %s', failure)

        elif failure.check(TimeoutError, TCPTimedOutError):
            self.logger.error('TimeoutError on %s', failure)

        # Reissuing a request
        yield scrapy.Request(failure, dont_filter=True, callback=self.get_page_links)


# Main program
process = CrawlerProcess(get_project_settings())
process.crawl(ListCrawler)
process.start()
