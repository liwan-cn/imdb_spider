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
    global ReviewDataPath, MovieIdDataPath, FinishedMivieIdPath
    global proxyHost, proxyPort, proxyUser, proxyPass, proxyMeta
    global proxies

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

    ReviewDataPath = "E:/Data/Movie/IMDb/movie_review_imdb.csv"
    MovieIdDataPath = "E:/Data/Movie/IMDb/movie_id.csv"
    FinishedMivieIdPath = "E:/Data/Movie/IMDb/finished.csv"

    proxyHost = "ip2.hahado.cn"
    proxyPort = "41790"
    proxyUser = "duoipoutcoxqq"
    proxyPass = "LZya6cTv8ZziA"
    # duoipoutcoxqq   LZya6cTv8ZziA@ip2.hahado.cn:41790
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
    }
    proxies = {
        "http": proxyMeta,
        "https": proxyMeta,
    }

def save(obj, fileName):
    """
    存储爬取状态
    :param obj:  对象名
    :param fileName: 存储文件
    :return: None
    """
    lock.acquire()
    output = open(fileName, 'wb')
    pickle.dump(obj, output)
    output.close()
    lock.release()

def load(fileName):
    """
    加载爬取状态
    :param fileName: 文件名
    :return: 爬取状态对应的字典
    """
    lock.acquire()
    if os.path.exists(fileName):
        input = open(fileName, 'rb')
        data = pickle.load(input)
        input.close()
    else:
        data = {}
    lock.release()
    return data

def getAllPageReview(movieId):
    """
    获取一部电影的所有评论
    :param movieId:电影ID
    :return: None
    """
    startTime = time.time()
    # 爬取状态为一个Python字典对象
    # key为movieId
    # value为一个List，List的最后一项为该部电影最后爬取并存储的位置
    crawlingStatus = load("crawling_status.pkl") #先加载爬取状态
    start = 0
    if movieId in crawlingStatus: #说明这部电影之前爬取了一部分
        if len(crawlingStatus[movieId]) > 0:
            start = crawlingStatus[movieId][-1]
    allPageReviewList = []
    length = 10
    # 初始化length = 10
    # imdb评论页面有10条评论
    # 如果length < 10 说明已经爬取到了该电影的所有评论
    while length == 10:
        url = "http://www.imdb.com/title/%s/reviews?start=%d" % (movieId, start)
        length, onePageReviewList = getOnePageReview(movieId, url, start)
        allPageReviewList.extend(onePageReviewList)
        start += 10 # 加10
        if start % 100 == 0 and start > 10: # 每100条评论就存入文件并更新爬取状态
            saveInfo(allPageReviewList)
            if movieId in crawlingStatus:
                crawlingStatus[movieId].append(start) #爬取状态更新
            else:
                crawlingStatus[movieId] = [start] #爬取状态加入
            save(crawlingStatus, "crawling_status.pkl")
            allPageReviewList = []
    endTime = time.time()
    saveInfo(allPageReviewList) # 不到100条也存储
    # 存储已经爬取完毕的movieID
    lock.acquire()
    with open(FinishedMivieIdPath, 'a', encoding="utf-8", newline="") as f:  # 必须用'a'模式打开
        writer_review = csv.writer(f)  # 创建写对象
        writer_review.writerow([movieId])
    lock.release()
    print("ID 为 %s 的电影爬取完成，共 %d 条评论，用时%d秒，完成时间：%s。" % (movieId, start + length, endTime-startTime, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTime))))

def requestsURL(url, headers, proxies):
    """
    http请求
    :param url: 页面url
    :param headers: 请求头部
    :param proxies: ip代理
    :return: 请求到的html
    """
    try:
        return requests.get(url, headers=headers, proxies=proxies)
    except: #出错就继续请求
        print("URL : %s Requests Error" % (url))
        return requestsURL(url, headers, proxies)

def getOnePageReview(movieId, url, start):
    """
    获得一个页面的评论
    :param url: 页面url
    :return: 该页面评论数和评论
    """
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    html = requestsURL(url, headers, proxies)
    length = 0
    allReviewCount = []
    if html.status_code == 200:
        text = html.text
        user = etree.HTML(text).xpath('//*[@id="tn15content"]/div/a[1]/@href')
        length = len(user)
        if start == 0 and length == 0:
            allReviewCount = etree.HTML(text).xpath('//*[@id="tn15content"]/table[1]/tr/td[2]/text()')
        else:
            allReviewCount = etree.HTML(text).xpath('//*[@id="tn15content"]/table[2]/tr/td[2]/text()')
    reTryTime = 0
    
    while length == 0 and reTryTime <= 12:
        # 如果该页面有0条评论说明已经爬取（总评论数为10的整数倍）完成或者页面返回错误
        # y页面返回错去就要sleep后重试 防止IP被封
        if len(allReviewCount) == 0:
            break
        elif int(allReviewCount[0].strip().split(' ')[0]) <= start:
            break
        time.sleep(5)
        html = requestsURL(url, headers, proxies)
        length = 0
        if html.status_code == 200:
            text = html.text
            user = etree.HTML(text).xpath('//*[@id="tn15content"]/div/a[1]/@href')
            length = len(user)
            if start == 0 and length == 0:
                allReviewCount = etree.HTML(text).xpath('//*[@id="tn15content"]/table[1]/tr/td[2]/text()')
            else:
                allReviewCount = etree.HTML(text).xpath('//*[@id="tn15content"]/table[2]/tr/td[2]/text()')
        reTryTime += 1
        print("URL %s ReTry Time : %d" % (url, reTryTime))
    print(url, html.status_code)
    onePageReviewList = []
    # print(length)
    for i in range(length):
        #  解析页面
        userWithUser = etree.HTML(text).xpath('//*[@id="tn15content"]/div[%d]/a[1]/@href' % (2 * i + 1))
        reviewLines = etree.HTML(text).xpath('//*[@id="tn15content"]/p[%d]/text()' % (i + 1))
        scoreOfTen = etree.HTML(text).xpath('//*[@id="tn15content"]/div[%d]/img/@alt' % (2 * i + 1))
        reviewTimeAtLast = etree.HTML(text).xpath('//*[@id="tn15content"]/div[%d]/small/text()' % (2 * i + 1))
        if (len(userWithUser) == 0):
            continue
        if (len(scoreOfTen) == 0):
            score = None
        else:
            score = scoreOfTen[0].split('/')[0]
        if (len(reviewTimeAtLast) == 0):
            reviewTime = None
        else:
            reviewTime = reviewTimeAtLast[-1]
        user = userWithUser[0].split('/')[2]
        review = ""
        for eachLine in reviewLines:
            review += eachLine.replace('\n', '').replace('\r', '').replace(',', ';')
        # print([movieId, user, score, reviewTime])
        onePageReviewList.append([movieId, user, score, reviewTime, review])
    print("This Page Have %d Review." % (length))
    return length, onePageReviewList #评论数=0说明所有评论读取完毕

def saveInfo(data):
    """
    存储评论数据
    :param data: 数据对象
    :return: None
    """
    lock.acquire()
    with open(ReviewDataPath, 'a', encoding="utf-8", newline="") as f:  # 必须用'a'模式打开
        writer_review = csv.writer(f)  # 创建写对象
        for item in data:
            writer_review.writerow(item)
    lock.release()

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

def initlock(l):
    """
    进程池全局锁
    :param l: 锁
    :return: None
    """
    global lock
    lock = l

def main():
    """
    主函数
    :return: None
    """
    allMovieIdSet = set(getMovieId(MovieIdDataPath)) # 所有的电影ID
    finishedMovieIdSet = set(getMovieId(FinishedMivieIdPath)) #已完成的电影ID
    if len(finishedMovieIdSet) == 0:
    # 已完成电影为0 就要在评论文件写入一行
        with open(ReviewDataPath, 'a', encoding="utf-8", newline="") as f:
            writer_review = csv.writer(f)  # 创建写对象
            writer_review.writerow(["movie_id", "reviewer_id", "score", "review_time", "review"])
    unFinishedMovieIdList = list(allMovieIdSet - finishedMovieIdSet) # 求未完成电影的列表
    lock = Lock() #进程池全局锁
    pool = Pool(40, initializer=initlock, initargs=(lock,)) #创建进程池
    pool.map(getAllPageReview, unFinishedMovieIdList) #把任务塞入进程池
    pool.close()  # 关闭进程池，不再接受新的进程
    pool.join()  # 主进程阻塞等待子进程的退出

def test():
    """
    测试有多少未爬完
    在跑爬虫时用不到
    :return: None
    """
    allMovieId = set(getMovieId(MovieIdDataPath))
    finishedMovieId = set(getMovieId(FinishedMivieIdPath))
    if len(finishedMovieId) == 0:
        with open(ReviewDataPath, 'a', encoding="utf-8", newline="") as f:
            writer_review = csv.writer(f)  # 创建写对象
            writer_review.writerow(["movieId", "reviewerId", "score", "reviewTime", "review"])
    unFinishedMovieIdList = list(allMovieId - finishedMovieId)
    print(len(allMovieId), allMovieId)
    print(len(unFinishedMovieIdList), unFinishedMovieIdList)

init()

if __name__ == "__main__" :
    main()
    #test()
    print("All Finished %s!" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))