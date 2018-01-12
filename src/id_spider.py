import requests
import time
import csv
import os
import random
import pickle
import lxml.etree as etree
from multiprocessing import Lock, Pool

def init():
    """
    初始化所有的去全局变量
    :return: None
    """
    global USER_AGENTS
    global MovieIdPath
    global Genres

    USER_AGENTS = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    ]

    MovieIdPath = "movie_id.csv"
    Genres = ["action", "adventure", "animation", "biography", "comedy", "crime", "documentary", "drama", "family", "fantasy", "film_noir", "history", "horror", "music", "musical", "mystery", "romance", "sci_fi", "sport", "thriller", "war", "western"]

def getInfo(genre):
    """
    获取一部电影的信息
    :param movieId:电影ID
    :return: None
    """
    for page in range(200):
        url = "http://www.imdb.com/search/title?genres=%s&view=simple&page=%d" % (genre, page+1)
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        html = requestsURL(url, headers)
        reTryTime = 0
        while html.status_code != 200 and reTryTime <= 12:
            time.sleep(5)
            html = requestsURL(url, headers)
            reTryTime += 1
            print("URL %s ReTry Time : %d" % (url, reTryTime))
        print(url, html.status_code)

        text = html.text
        movie_ids = etree.HTML(text).xpath('//*[@id="main"]/div/div/div[3]/div/div[2]/div/div[1]/span/span[2]/a/@href')
        for i in range(len(movie_ids)):
            movie_ids[i] = movie_ids[i].split("/")[2]
        saveInfo(movie_ids) 
        # print(movie_ids)
        # 存储已经爬取完毕的movieID

def requestsURL(url, headers):
    """
    http请求
    :param url: 页面url
    :param headers: 请求头部
    :return: 请求到的html
    """
    try:
        return requests.get(url, headers=headers)
    except: #出错就继续请求
        print("URL : %s Requests Error" % (url))
        return requestsURL(url, headers)


def saveInfo(data):
    """
    存储评论数据
    :param data: 数据对象
    :return: None
    """
    with open(MovieIdPath, 'a', encoding="utf-8", newline="") as f:  # 必须用'a'模式打开
        csv_writer = csv.writer(f)  # 创建写对象
        for item in data:
            csv_writer.writerow([item])



def main():
    """
    主函数
    :return: None
    """
    for genre in Genres:
        getInfo(genre)


init()

if __name__ == "__main__" :
    main()
    print("All Finished %s!" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))