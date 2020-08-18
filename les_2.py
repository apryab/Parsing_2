import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime


class GBlogParse:
    __domain = "https://geekbrains.ru"
    __start_url = "https://geekbrains.ru/posts"

    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017')
        self.db = self.client['parse_gb_blog']
        self.collection = self.db['posts']
        self.visited_urls = set()
        self.post_links = set()
        self.posts_data = []

    def parse_rows(self, url=__start_url):
        count = 0
        while url:
            if url in self.visited_urls:
                break
            response = requests.get(url)
            self.visited_urls.add(url)
            soap = BeautifulSoup(response.text, 'lxml')
            url = self.get_next_page(soap)
            self.search_post_links(soap)
            print('loop', count, 'done')
            count += 1

    def get_next_page(self, soap):
        ul = soap.find('ul', attrs={'class': 'gb__pagination'})
        a = ul.find('a', text=re.compile('›'))
        return f'{self.__domain}{a.get("href")}' if a and a.get("href") else None

    def search_post_links(self, soap):
        wrapper = soap.find('div', attrs={'class': 'post-items-wrapper'})
        posts = wrapper.find_all('div', attrs={'class': 'post-item'})
        links = [f'{self.__domain}{itm.find("a").get("href")}' for itm in posts]
        self.post_links.update(links)

    def post_page_parse(self):
        count = 0
        for url in self.post_links:
            if url in self.visited_urls:
                continue
            response = requests.get(url)
            self.visited_urls.add(url)
            soap = BeautifulSoup(response.text, 'lxml')
            self.posts_data.append(self.get_post_data(soap, url))
            print('link №', count, 'gained')
            count += 1

    def get_post_data(self, soap, url):
        result = {'url': url, 'title': soap.find('h1', attrs={'class': 'blogpost-title'}).text}
        content = soap.find('div', attrs={'class': 'blogpost-content', 'itemprop': 'articleBody'})
        img = content.find('img')
        result['image'] = img.get('src') if img else None
        author_block = soap.find('div', attrs={'class': 'row m-t'})
        author = author_block.find('div', attrs={'itemprop': "author"})
        author_name = author.text
        result['author_name'] = author_name
        author_link = author_block.find('a').get('href')
        result['author_url'] = (self.__domain + author_link) if author_link else None
        time_block = soap.find('div', attrs={'class': 'blogpost-date-views'})
        time = time_block.find('time').get('datetime')
        time_date = time[0:10]
        time_time = time[11:19]
        time_string = time_date + ' ' + time_time
        time_string = time_string.replace('-', ' ')
        time_string = time_string.replace(':', ' ')
        time_format = datetime.strptime(time_string, '%Y %m %d %H %M %S')
        result['datetime'] = time_format
        return result

    def save_to_mongo(self):
        self.collection.insert_many(self.posts_data)

    def download_from_mongo(self):
        print('Введите начало диапазона (формат - %Y %m %d %H %M %S)')
        date_from = input()
        print('Введите конец диапазона (формат - %Y %m %d %H %M %S)')
        date_to = input()
        date_from = datetime.strptime(date_from, '%Y %m %d %H %M %S')
        date_to = datetime.strptime(date_to, '%Y %m %d %H %M %S')
        result = list(self.collection.find({'$and': [{'datetime': {'$gte': date_from}},
                                                     {'datetime': {'$lte': date_to}}]}))
        print(result)


parser = GBlogParse()
parser.parse_rows()
parser.post_page_parse()
parser.save_to_mongo()
parser.download_from_mongo()
