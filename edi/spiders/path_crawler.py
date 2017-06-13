import urllib2
import scrapy
import re
import cgi
import os
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


class Links(scrapy.Item):
    """
    This class defines the fields for storing the metadata in between requests

    """
    main_url = scrapy.Field()
    current_depth = scrapy.Field()
    current_url_pattern = scrapy.Field()
    total_depth = scrapy.Field()
    depth_url_patterns = scrapy.Field()
    depth_url_patterns_rev = scrapy.Field()


class PathCrawler(scrapy.Spider):
    # Spider's name
    name = "data"

    # Start page's URL
    start_urls = ['https://portal.edirepository.org/nis/browseServlet?searchValue=water+balance']

    # Create a parent directory for storing the downloaded files
    directory = start_urls[0].split("=")[-1]
    if not os.path.exists(directory):
        os.makedirs(directory)

    def parse(self, response):
        """
        The starting point of the crawler. Parses the start URL and follows the subsequent requests
        :param response: The response packet received after making a request to the server

        """
        # Get the total number of depths (levels) from the user
        total_depth = int(raw_input("Enter max depth: "))

        # Two maps which map the depth with the URL pattern and vice-versa
        url_pattern_for_each_depth = {}
        url_pattern_for_each_depth_rev = {}

        # Get the URL pattern for each depth from the user
        for i in range(0, total_depth):
            depth_url = raw_input("Specify URL pattern for depth %s: " % (i + 1))
            url_pattern_for_each_depth[depth_url] = i
            url_pattern_for_each_depth_rev[i] = depth_url

        # Storing metadata
        item = Links()
        item['current_depth'] = 0
        item['total_depth'] = total_depth

        item['depth_url_patterns'] = url_pattern_for_each_depth
        item['depth_url_patterns_rev'] = url_pattern_for_each_depth_rev
        item['current_url_pattern'] = " "

        # Creating a request to the URL in 'response.url' which will be handled by the function
        # get_table_links. self.errback will handle any exceptions that occur during crawling.
        request = scrapy.Request(response.url, callback=self.get_table_links, errback=self.errback)
        request.meta['item'] = item
        yield request

    def eliminate_duplicates(self, list_with_duplicates):
        """
        Eliminates duplicates from a list
        :param list_with_duplicates: The input list with duplicates
        :return: List with unique elements

        """
        # Dictionary to record seen items
        seen = {}
        result = []
        for item in list_with_duplicates:
            if item in seen:
                continue
            else:
                # Marking the item as seen
                seen[item] = 1
                result.append(item)
        return result

    def match_regex_in_list(self, regex, list_of_items):
        """
        Applies a regular expression to a list and extracts all the items which satisfy the pattern
        :param regex: The pattern to search for in the list
        :param list_of_items: Input list
        :return: List of matches
        """
        result = [match.group(0) for match in list_of_items for match in [regex.search(match)] if match]
        return result

    def get_table_links(self, response):
        """
        Forms the URL for each depth using the URL pattern and recursively crawls each depth till the
        final URL is reached
        :param response: The response packet received after making a request to the server

        """
        # Getting the metadata from the response
        item = response.meta['item']

        # Finding the depth from the URL pattern
        for pattern in item['depth_url_patterns']:
            if pattern in response.url:
                item['current_depth'] = (item['depth_url_patterns'][pattern]) + 1

        item['current_url_pattern'] = item['depth_url_patterns_rev'][item['current_depth']]
        url_pattern = item['current_url_pattern']

        # Extracting all the URLs matching the URL pattern
        approx_match_url_paths = response.xpath('//a[contains(@href,' + "\"" + url_pattern + '\")]/@href').extract()
        if approx_match_url_paths:
            regex = re.compile("\\b" + re.escape(url_pattern) + ".*")
            exact_match_url_paths = self.match_regex_in_list(regex, approx_match_url_paths)

            matched_url_paths = self.eliminate_duplicates(exact_match_url_paths)
            if matched_url_paths:
                for url_path in matched_url_paths:

                    # Forming the full URL using the parent URL and the URL pattern
                    url_href = response.urljoin(url_path)

                    # Create download request if the last depth URL is reached
                    if item['current_depth'] == item['total_depth'] - 1:
                        request = scrapy.Request(url_href, callback=self.download_data_files, errback=self.errback)
                    else:
                        # Recursively traverse to the next depth
                        request = scrapy.Request(url_href, callback=self.get_table_links, errback=self.errback)
                    request.meta['item'] = item
                    yield request

    def download_data_files(self, response):
        """
        Creates a sub directory for the file based on the URL and saves the file in it
        :param response: The response packet received after making a request to the server

        """
        # Opens URL and gets the filename
        http_response = urllib2.urlopen(response.url)
        _, params = cgi.parse_header(http_response.headers.get('Content-Disposition', ''))
        filename = params['filename']

        # Creates a subdirectory and saves the file
        PathCrawler.package_name = re.search("packageid=(.*)&", response.url).group(1)
        PathCrawler.package_name = os.path.join(PathCrawler.directory + os.sep, PathCrawler.package_name)
        if not os.path.exists(PathCrawler.package_name):
            os.makedirs(PathCrawler.package_name)
        with open(os.path.join(PathCrawler.package_name, filename), 'wb+') as file:
            self.logger.info("Saving file %s/%s", PathCrawler.package_name, filename)
            file.write(response.body)
        file.close()

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

# Main program
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
process.crawl(PathCrawler)
process.start()
