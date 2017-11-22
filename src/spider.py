import requests
import time
import csv
import random
import lxml.etree as etree
from multiprocessing import Lock, Pool

def init():
    global USER_AGENTS
    global ReviewDataPath, MovieIdDataPath, FinishedMivieIdPath
    global proxyHost, proxyPort, proxyUser, proxyPass, proxyMeta
    global proxies
    # duoipqfnhqhkg:mBiXA5FnZ6bNG@ip2.hahado.cn:41400

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

    ReviewDataPath = "movie_review.csv"
    MovieIdDataPath = "movie_id.csv"
    FinishedMivieIdPath = "finished.csv"

    proxyHost = "ip2.hahado.cn"
    proxyPort = "41400"
    proxyUser = "duoipqfnhqhkg"
    proxyPass = "mBiXA5FnZ6bNG"
    # duoipqfnhqhkg:mBiXA5FnZ6bNG@ip2.hahado.cn:41400
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

def getAllPageReview(movieId):
    """
    获取一部电影的所有评论
    :param movieId:电影ID
    :return: None
    """
    startTime = time.time()
    allPageReviewList = []
    flag = False
    start = 0
    while flag == False:
        url = "http://www.imdb.com/title/%s/reviews?start=%d" % (movieId, start)
        flag, onePageReviewList = getOnePageReview(movieId, url)
        allPageReviewList.extend(onePageReviewList)
        start += 10
        # if start % 500 == 0:
        #     saveInfo(allPageReviewList)
        #     allPageReviewList = []
    endTime = time.time()
    lock.acquire()
    with open(FinishedMivieIdPath, 'a', encoding="utf-8", newline="") as f:  # 必须用'a'模式打开
        writer_review = csv.writer(f)  # 创建写对象
        writer_review.writerow([movieId])
    lock.release()
    print("ID 为 %s 的电影爬取完成，共 %d 条评论，用时%d秒，完成时间：%s。" % (movieId, len(allPageReviewList), endTime-startTime, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTime))))
    saveInfo(allPageReviewList)

def requestsURL(url, headers, proxies):
    try:
        return requests.get(url, headers=headers, proxies=proxies)
    except:
        print("URL : %s Requests Error" % (url))
        return requestsURL(url, headers, proxies)

def getOnePageReview(movieId, url):
    """
    获得一个页面的所有评论
    :param url: 页面url
    :return: True or False
    """
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    html = requestsURL(url, headers, proxies)
    length = 0
    if html.status_code == 200:
        text = html.text
        user = etree.HTML(text).xpath('//*[@id="tn15content"]/div/a[1]/@href')
        length = len(user)
    reTryTime = 0
    while length == 0 and reTryTime <= 12:
        time.sleep(5)
        html = requestsURL(url, headers, proxies)
        length = 0
        if html.status_code == 200:
            text = html.text
            user = etree.HTML(text).xpath('//*[@id="tn15content"]/div/a[1]/@href')
            length = len(user)
        reTryTime += 1
        print("URL %s ReTry Time : %d" % (url, reTryTime))
    print(url, html.status_code)
    onePageReviewList = []
    # print(length)
    for i in range(length):
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
    return (length < 10 ), onePageReviewList #评论数=0说明所有评论读取完毕

def saveInfo(data):
    lock.acquire()
    with open(ReviewDataPath, 'a', encoding="utf-8", newline="") as f:  # 必须用'a'模式打开
        writer_review = csv.writer(f)  # 创建写对象
        for item in data:
            writer_review.writerow(item)
    lock.release()

def getMovieId(fileName):
    MovieIdList =[]
    with open(fileName, 'r', encoding="utf-8") as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            MovieIdList.append(row[0])
    return MovieIdList

def initlock(l):
    global lock
    lock = l

def main():
    allMovieIdSet = set(getMovieId(MovieIdDataPath))
    finishedMovieIdSet = set(getMovieId(FinishedMivieIdPath))
    if len(finishedMovieIdSet) == 0:
        with open(ReviewDataPath, 'a', encoding="utf-8", newline="") as f:
            writer_review = csv.writer(f)  # 创建写对象
            writer_review.writerow(["movieId", "reviewerId", "score", "reviewTime", "review"])
    unFinishedMovieIdList = list(allMovieIdSet - finishedMovieIdSet)
    lock = Lock()
    pool = Pool(16, initializer=initlock, initargs=(lock,))
    pool.map(getAllPageReview, unFinishedMovieIdList)
    pool.close()  # 关闭进程池，不再接受新的进程
    pool.join()  # 主进程阻塞等待子进程的退出

def test():
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