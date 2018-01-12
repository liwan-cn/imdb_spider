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
    global MovieDataPath, MovieIdDataPath, FinishedMivieIdPath


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

    MovieDataPath = "movie_info.csv"
    MovieIdDataPath = "movie_id.csv"
    FinishedMivieIdPath = "finished.csv"


def getInfo(movieId):
    """
    获取一部电影的信息
    :param movieId:电影ID
    :return: None
    """
    url = "http://www.imdb.com/title/%s/" % (movieId)
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

    info = [movieId]
    movie_name = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[2]/div[2]/div/div[2]/div[2]/h1/text()')
    genre = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[2]/div[2]/div/div[2]/div[2]/div/a/span/text()')
    director = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[3]/div[1]/div[2]/span/a/span/text()')
    if len(director) == 0:
        director = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[3]/div[2]/div[1]/div[2]/span/a/span/text()')
    writers = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[3]/div[1]/div[3]/span/a/span/text()')
    if len(writers) == 0:
        writers = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[3]/div[2]/div[1]/div[3]/span/a/span/text()')
    stars = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[3]/div[1]/div[4]/span/a/span/text()')
    if len(stars) == 0:
        stars = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[3]/div[2]/div[1]/div[4]/span/a/span/text()')
    country = etree.HTML(text).xpath('//*[@id="titleDetails"]/div[2]/a/text()')
    release_year = etree.HTML(text).xpath('//*[@id="titleYear"]/a/text()')
    movie_time = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[2]/div[2]/div/div[2]/div[2]/div/time/text()')
    score = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[2]/div[2]/div/div[1]/div[1]/div[1]/strong/span/text()')
    score_count = etree.HTML(text).xpath('//*[@id="title-overview-widget"]/div[2]/div[2]/div/div[1]/div[1]/a/span/text()')

    if len(movie_name) != 0:
        info.append(movie_name[0].strip())
    else:
        info.append('')

    info.append('|'.join(genre))
    info.append('|'.join(director))
    info.append('|'.join(writers))
    info.append('|'.join(stars))
    info.append('|'.join(country))

    if len(release_year) != 0:
        info.append(release_year[0].strip())
    else:
        info.append('')

    if len(movie_time) != 0:
        info.append(movie_time[0].strip())
    else:
        info.append('')

    if len(score) != 0:
        info.append(score[0])
    else:
        info.append('')
    if len(score_count) != 0:
        info.append(score_count[0].replace(',',''))
    else:
        info.append('')
    # print(info)

    saveInfo(info) 
    # 存储已经爬取完毕的movieID

    with open(FinishedMivieIdPath, 'a', encoding="utf-8", newline="") as f:  # 必须用'a'模式打开
        writer_review = csv.writer(f)  # 创建写对象
        writer_review.writerow([movieId])

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
    with open(MovieDataPath, 'a', encoding="utf-8", newline="") as f:  # 必须用'a'模式打开
        csv_writer = csv.writer(f)  # 创建写对象
        csv_writer.writerow(data)

def getMovieId(fileName):
    """
    获取mivieId
    :param fileName: 文件名
    :return: movieId的List
    """
    MovieIdList =[]
    with open(fileName, 'r', encoding="utf-8") as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            MovieIdList.append(row[0])
    return MovieIdList

def main():
    """
    主函数
    :return: None
    """
    allMovieIdSet = set(getMovieId(MovieIdDataPath)) # 所有的电影ID
    print(len(allMovieIdSet))
    finishedMovieIdSet = set(getMovieId(FinishedMivieIdPath)) #已完成的电影ID
    if len(finishedMovieIdSet) == 0:
    # 已完成电影为0 就要在评论文件写入一行
        with open(MovieDataPath, 'a', encoding="utf-8", newline="") as f:
            writer_review = csv.writer(f)  # 创建写对象
            writer_review.writerow(["movie_id", "movie_name", "genre", "director", "writers", "actors", "country", "release_year", "movie_time", "score", "score_count"])
    unFinishedMovieIdList = list(allMovieIdSet - finishedMovieIdSet) # 求未完成电影的列表
    print(len(unFinishedMovieIdList))
    for movieId in unFinishedMovieIdList:
        getInfo(movieId)


init()

if __name__ == "__main__" :
    main()
    print("All Finished %s!" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))