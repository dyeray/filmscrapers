import scrapy


class WeirdSpider(scrapy.Spider):
    name = 'weird'
    allowed_domains = ['366weirdmovies.com']
    start_urls = ['http://366weirdmovies.com/']

    def parse(self, response, **kwargs):
        urls = response.xpath('//*[@id="the-list-canonical-weird-movies"]/parent::*/following-sibling::aside[1]//a/@href').extract()
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_item)

    def parse_item(self, response, **kwargs):
        pagetitle = response.css('h1.entry-title::text').get()
        number, title = pagetitle.split('.', 1)
        yield {
            'number': number.strip(),
            'title': title.strip()
        }
