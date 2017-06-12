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
    main_url = scrapy.Field()
    current_depth = scrapy.Field()
    current_url_pattern = scrapy.Field()
    total_depth = scrapy.Field()
    depth_url_pattern = scrapy.Field()
    all_urls = scrapy.Field()


class PathCrawler(scrapy.Spider):
    # Spider's name
    name = "data"
    # Start page's URL
    start_urls = ['https://portal.edirepository.org/nis/browseServlet?searchValue=water+balance']

    # Create a directory
    directory = start_urls[0].split("=")[-1]
    if not os.path.exists(directory):
        os.makedirs(directory)

    def parse(self, response):
        total_depth = int(raw_input("Enter max depth: "))
        url_pattern_for_each_depth = ["mapbrowse?packageid=", 'dataviewer?packageid=']
        for i in range(1, total_depth + 1):
            depth_url = raw_input("Specify URL pattern for depth %s: " % i)
            url_pattern_for_each_depth.append(depth_url)

        item = Links()
        item['current_depth'] = 0
        item['total_depth'] = total_depth

        item['depth_url_pattern'] = url_pattern_for_each_depth
        item['current_url_pattern'] = " "
        item['all_urls'] = set()
        item['all_urls'].add(item['main_url'][0])
        request = scrapy.Request(response.url, callback=self.get_table_links, errback=self.errback)
        request.meta['item'] = item
        yield request

    def eliminate_duplicates(self, list_with_duplicates):
        seen = {}
        result = []
        for item in list_with_duplicates:
            if item in seen:
                continue
            else:
                seen[item] = 1
                result.append(item)
        return result

    def match_regex_in_list(self, regex, list_of_items):
        result = [match.group(0) for match in list_of_items for match in [regex.search(match)] if match]
        return result

    def get_table_links(self, response):
        item = response.meta['item']
        for pattern in item['depth_url_pattern']:
            if pattern in response.url:
                item['current_depth'] = (item['depth_url_pattern'].index(pattern)) + 1

        item['current_url_pattern'] = item['depth_url_pattern'][item['current_depth']]
        url_pattern = item['current_url_pattern']
        approx_match_url_paths = response.xpath('//a[contains(@href,' + "\"" + url_pattern + '\")]/@href').extract()

        if approx_match_url_paths:
            regex = re.compile("\\b" + re.escape(url_pattern) + ".*")
            exact_match_url_paths = self.match_regex_in_list(regex, approx_match_url_paths)

            matched_url_paths = self.eliminate_duplicates(exact_match_url_paths)
            if matched_url_paths:
                for url_path in matched_url_paths:
                    url_href = response.urljoin(url_path)
                    item['all_urls'].add(url_href)
                    if item['current_depth'] == item['total_depth'] - 1:
                        request = scrapy.Request(url_href, callback=self.download_data_files, errback=self.errback)
                    else:
                        request = scrapy.Request(url_href, callback=self.get_table_links, errback=self.errback)
                    request.meta['item'] = item
                    yield request

    def download_data_files(self, response):
        http_response = urllib2.urlopen(response.url)
        _, params = cgi.parse_header(http_response.headers.get('Content-Disposition', ''))
        filename = params['filename']
        PathCrawler.package_name = re.search("packageid=(.*)&", response.url).group(1)
        PathCrawler.package_name = os.path.join(PathCrawler.directory + os.sep, PathCrawler.package_name)
        if not os.path.exists(PathCrawler.package_name):
            os.makedirs(PathCrawler.package_name)
        with open(os.path.join(PathCrawler.package_name, filename), 'wb') as file:
            self.logger.info("Saving file %s/%s", PathCrawler.package_name, filename)
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
process.crawl(PathCrawler)
process.start()
