import html
import json
import re
from csv import DictReader
from typing import Optional

import scrapy
from scrapy.selector import SelectorList, Selector

from filmscrapers.utils import first


def get_ids_from_csv(filepath: str):
    with open(filepath) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            yield row['Const']


class ImdbSpider(scrapy.Spider):
    name = 'imdb'
    allowed_domains = ['imdb.com']

    def req_film(self, imdb_id: str):
        return scrapy.Request(
            url=f'https://www.imdb.com/title/{imdb_id}/',
            callback=self.parse_film,
            meta={'imdb_id': imdb_id}
        )

    def req_person(self, imdb_id: str):
        return scrapy.Request(
            url=f'https://www.imdb.com/name/{imdb_id}/',
            callback=self.parse_person,
            meta={'imdb_id': imdb_id}
        )

    def start_requests(self):
        return [self.req_film(imdb_id) for imdb_id in get_ids_from_csv(self.filepath)]

    def parse_countries(self, countries: SelectorList):
        return [self.parse_country(country) for country in countries]

    def parse_country(self, country: Selector):
        url = country.css('::attr(href)').get()
        regex = r'country_of_origin=(\w+)\&'
        return {
            'code': first(re.findall(regex, url)),
            'name': country.css('::text').get()
        }

    def parse_languages(self, languages: SelectorList):
        return [self.parse_language(language) for language in languages]

    def parse_language(self, language: Selector):
        url = language.css('::attr(href)').get()
        regex = r'primary_language=(\w+)\&'
        return {
            'code': first(re.findall(regex, url)),
            'name': language.css('::text').get()
        }

    def parse_film(self, response):
        match = first(re.findall(r'<script type="application/ld\+json">(.*?)</script>', response.text))
        if not match:
            return
        data = json.loads(match)
        director_ids = self.get_ids_from_people(data.get('director'))
        writer_urls = response.xpath('(//*/li[@role="presentation"][span[contains(text(), "Writer")]])[1]//a/@href').getall()
        writer_ids = [self.get_id_from_url(url) for url in writer_urls]
        cast_urls = response.css('section div[data-testid="title-cast-item"] a[data-testid="title-cast-item__actor"]::attr(href)').getall()
        cast_ids = [self.get_id_from_url(url) for url in cast_urls]
        yield {
            'imdb_id': response.meta['imdb_id'],
            'imdb_rating': data['aggregateRating']['ratingValue'] if 'aggregateRating' in data else None,
            'imdb_votes': data['aggregateRating']['ratingCount'] if 'aggregateRating' in data else 0,
            'work_type': data['@type'],
            'title': html.unescape(data['name']),
            'director_ids': director_ids,
            'writer_ids': writer_ids,
            'cast_ids': cast_ids,
            'countries': self.parse_countries(response.css('li[data-testid="title-details-origin"] div')),
            'genres': data['genre'],
            'languages': self.parse_languages(response.css('li[data-testid="title-details-languages"] div')),
            'length': self.get_duration(data),
            'year': self.get_year(data, response.text),
            'image': data.get('image', None)
        }

        people = set(director_ids + writer_ids + cast_ids)

        for person in people:
            yield self.req_person(person)

    def parse_person(self, response, **kwargs):
        match = first(re.findall(r'<script type="application/ld\+json">(.*?)</script>', response.text, re.DOTALL))
        if not match:
            return
        data = json.loads(match)
        yield {
            'imdb_id': response.meta['imdb_id'],
            'name': data['name'],
            'birth_date': data.get('birthDate'),
            'born_place': response.xpath('//*[@id="name-born-info"]//a[contains(@href, "birth_place=")]/text()').get(),
            'image': data.get('image', None)
        }

    def get_duration(self, data):
        duration_str = data.get('duration')
        if not duration_str:
            return None
        match = re.match(r'PT(\d+)H(\d+)M', duration_str)
        return int(match.group(1)) * 60 + int(match.group(2)) if match else None

    def get_year(self, data, text) -> Optional[int]:
        publish_date = data.get('datePublished', None)
        json_date = first(publish_date.split('-', 1) if publish_date else [])
        if json_date:
            return int(json_date)
        regex_year = first(re.findall(r'"releaseYear":{"year":(\d+),', text))
        return int(regex_year) if regex_year else None

    def get_ids_from_people(self, people):
        if not people:
            return []
        return [self.get_id_from_url(person['url']) for person in people]

    def get_id_from_url(self, url):
        return url.split('/')[2].split('?')[0]
