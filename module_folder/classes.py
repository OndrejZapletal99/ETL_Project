import bs4
import requests
import pandas as pd
import datetime as dt


class WebScraper:

    def __init__(self,url):

        self.url = url
        self.res = None
        self.soap = None
        self.articles = set()
        self.download_date = dt.datetime.now().strftime('%Y-%m-%d')

    def web_reader(self):

        self.res = requests.get(self.url)
        self.soap = bs4.BeautifulSoup(self.res.text, 'lxml')
    
    def article_find(self):

        for article in self.soap.select('.c_aM.c_aP'):
            title = article.get_text(strip = True)
            a_tag = article.find('a')
            if a_tag and 'href' in a_tag.attrs and title != '':
                link = a_tag['href']
                self.articles.add((title, link, self.download_date))

    def articles_to_df(self):
        
        df_articles = pd.DataFrame(self.articles, columns=['Title', 'Link', 'Download_Date'])
        return df_articles
