"""
目标网：http://dongyeguiwu.zuopinj.com/5525/
编写两个爬虫，爬虫1.从http://dongyeguiwu.zuopinj.com/5525/获取《白夜行》第一章到第十三章的网址
并将网址添加到Redis数据库中名为url_queue的列表中。
爬虫2从redis里名为url_queue的列表中读取网址，进入网址爬取每一章的具体内容。
再将内容保存到MongoDb中
"""
import redis
import requests
from lxml.html import etree
import  lxml.html
from pymongo import MongoClient
"""
1.request获取网页源代码
2.xpath获取源代码中的数据
3.redis与mongoDb的操作
"""
def get_href(url):
    """
    chapter:章节数
    href:章节链接
    :param source: 网页源码
    :return: chapter,href字典
    """
    response = requests.get(url,headers=headers)
    response.encoding = response.apparent_encoding
    source = response.text
    selector = lxml.html.fromstring(source)
    #selector = etree.HTML(source)
    # chapter_list = selector.xpath('//body/div/div/div/ul/li/a')
    href_list = selector.xpath('//body/div/div/div/ul/li/a/@href')
    return href_list


def save_redis(url_list):
    """
    chapter
    :param url_list:
    :return:
    """
    for url in url_list:
        try:
            client.lpush('url_queue',url)
        except Exception as e:
            print(e)
            print('插入redis数据库出现问题')


def get_article(href):
    """

    :param href:爬取文件的连接
    :return: str类型的文字内容
    """
    response = requests.get(url,headers = headers)
    response.encoding = response.apparent_encoding
    source = response.text
    seletor = lxml.html.fromstring(source)
    articles = seletor.xpath('//p/text()')
    chapter = seletor.xpath('//*[@id="jsnc_l"]/div[1]/h1/text()')
    for article in articles:
        article.rstrip()
    return chapter,articles


if __name__ == '__main__':
    headers = {
        'user-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'
    }
    # 起始页面
    url = 'http://dongyeguiwu.zuopinj.com/5525/'
    # 连接redis
    client = redis.StrictRedis()
    # 连接mongodb
    mongo_client = MongoClient('mongodb://localhost:27017')
    # 选择数据库
    database = mongo_client['Student']
    # 创建集合collection ,同SQL的表一样
    collection = database['baiyexing']
    # 提取url页面存储到redis
    href_list = get_href(url)
    save_redis(href_list)
    """
    对于爬取正文的爬虫，只要发现redis里的url_queue列表不为空，则从里面读取数据，并爬取数据。
    """
    content_list = []
    while client.llen('url_queue') >0:
        url = client.lpop('url_queue').decode()
        chapter,content = get_article(url)
        content_list.append({'title':chapter,'content':'\n'.join(content)})
    # 优化mongodb，避免频繁多次插入操作
    # 可以等数据够了一起插入
    try:
        collection.insert_many(content_list)
    except Exception as e:
        print(e)
        print("插入mongdb数据出现问题")