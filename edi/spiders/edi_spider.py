import urllib2

import scrapy
import re
import cgi
import os


class EdiSpider(scrapy.Spider):
    name = "edi"
    url = 'https://portal.edirepository.org/nis/browseServlet?searchValue=genomics'
    directory = url.split("=")[-1]
    if not os.path.exists(directory):
        os.makedirs(directory)

    data_package = ""

    def start_requests(self):
        yield scrapy.Request(self.url, self.parse)

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
                EdiSpider.data_package = data_lake_overview_page.split("=")[1]
                EdiSpider.data_package = os.path.join(EdiSpider.directory + os.sep, EdiSpider.data_package)
                if not os.path.exists(EdiSpider.data_package):
                    os.makedirs(EdiSpider.data_package)
                count += 1
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

        with open(os.path.join(EdiSpider.data_package, filename), 'wb') as file:
            self.logger.info("Saving file %s/%s", EdiSpider.data_package, filename)
            file.write(response.body)

from scrapy import cmdline
cmdline.execute("scrapy crawl edi".split())
