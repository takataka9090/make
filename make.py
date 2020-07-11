from pg.models import News

import schedule
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlparse
import time
import re
import urllib.robotparser

class WebsiteCrawler():
        
    def __init__(self, website_url, user_agent, limit_number):
        self.website_url = website_url
        self.user_agent = user_agent
        self.limit_number = limit_number
        self.all_url_list = [website_url]
        self.target_index = -1
        self.robotparser = urllib.robotparser.RobotFileParser()
        
    def __read_robots__(self):
        parsed = urlparse(self.website_url)
        robots_url = f'{parsed.scheme}://{parsed.netloc}/robots.txt'
        self.robotparser.set_url(robots_url)
        self.robotparser.read()
        
    def _clean_url_list(self, url_list):
        website_url_list = list(filter(
            lambda u: u.startswith(self.website_url), url_list
        ))
        non_flagment_url_list = [(
            lambda u: re.match('(.*?)#.*?', u) or re.match('(.*)', u)
        )(url).group(1) for url in website_url_list]
        
        return non_flagment_url_list
    
    def _extract_url(self, beautiful_soup):

        a_tag_list = list(filter(
            lambda tag: not 'nofollow' in (tag.get('rel') or ''),
            beautiful_soup.select('a')
        ))
        url_list = [
            urljoin(self.website_url, tag.get('href')) for tag in a_tag_list
        ]
        return self._clean_url_list(url_list)
    
    def crawl(self):
        print('クローリング開始')
        self.__read_robots__()
        while True:
            self.target_index += 1
            len_all = len(self.all_url_list)
            if self.target_index >= len_all or len_all > self.limit_number:
                print('クローリング終了')
                break
            self.target_index and time.sleep(2)
            print(f'{self.target_index + 1}ページ目開始')
            url = self.all_url_list[self.target_index]
            print(f'走査対象：{url}')
            if not self.robotparser.can_fetch(self.user_agent, url):
                print(f'{url}はアクセスが許可されていません')
                continue
            headers = {'User-Agent': self.user_agent}
            res = requests.get(url, headers=headers)
            if res.status_code != 200:
                print(f'通信に失敗しました（ステータス：{res.status_code}）')
                continue
            soup = BeautifulSoup(res.text, 'html.parser')
            for robots_meta in soup.select("meta[name='robots']"):
                if 'nofollow' in robots_meta['content']:
                    print(f'{url}内のリンクはアクセスが許可されていません')
                    continue
            cleaned_url_list = self._extract_url(soup)
            print(f'このページで収集したURL件数：{len(cleaned_url_list)}')
            before_extend_num = len(self.all_url_list)
            self.all_url_list.extend(cleaned_url_list)
            before_duplicates_num = len(self.all_url_list)
            self.all_url_list = sorted(
                set(self.all_url_list), key=self.all_url_list.index
            )
            after_duplicates_num = len(self.all_url_list)

            print(f'重複除去件数：{before_duplicates_num - after_duplicates_num}')
            print(f'URL追加件数：{after_duplicates_num - before_extend_num}')
        return self.all_url_list


if __name__ == '__main__':
    target_website = 'https://natalie.mu/music/news'
    user_agent = 'Mozilla/5.0 (the.rcst9.m13@icloud.com)'
    limit_number = 50
    crawler = WebsiteCrawler(target_website, user_agent, limit_number)
    result_url_list = crawler.crawl()
    news = result_url_list
    News.save()
    schedule.every(1).minutes.do( __name__ == '__main__')
    while True:
        schedule.run_pending()
        time.sleep(1)
    
    
        
    
    
    
    
