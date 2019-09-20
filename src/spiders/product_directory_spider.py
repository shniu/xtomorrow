# -*- coding: utf-8 -*-

import json
import collections
import requests
import time
import random
import pandas as pd
import click
from requests import ReadTimeout
from scrapy.selector import Selector

import sys
reload(sys)
sys.setdefaultencoding('utf8')

DEBUG = "debug"
DEV = "dev"
env = "publish"

# 产品目录入口地址
homepage = "http://www.stats.gov.cn"
directory_url_tpl = "{}/tjsj/tjbz/tjypflml/index{}.html"
directory_url_tpl2 = "{}{}"
directory_page_cnt = 5

# 任务队列
spider_task_queue = collections.deque()

# 最终结果存储在这里
result_list = []
err_list = []

user_agent = [
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
    "Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+",
    "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0",
    "Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)",
    "UCWEB7.0.2.37/28/999",
    "NOKIA5700/ UCWEB7.0.2.37/28/999",
    "Openwave/ UCWEB7.0.2.37/28/999",
    "Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999"

]


def format_url(tpl, *args):
    return tpl.format(homepage, *args)


def get_level1_directory(content, parent_path):
    """
    解析一级目录
    :param content: html 代码
    :return:
    """
    items = Selector(text=content) \
        .xpath("//ul[@class='center_list_contlist']/li/a")

    for item in items:
        name = item.xpath("./*/font[@class='cont_tit03']/text()").extract()[0]
        # name = name.encode("utf-8")

        # add url to spider_task_queue
        spider_task_queue.append({
            "level": 1,
            "url": format_url(directory_url_tpl2, item.xpath("./@href").extract()[0]),
            "name": name,
            "path": parent_path + "/" + name if parent_path != "" else name
        })

        result_list.append({
            "name": name,
            "level": 1,
            "parent": "",
            "leaf": False,
            "path": parent_path + "/" + name if parent_path != "" else name
        })

        if env == DEV:
            break
    print u"\t\t|- 提交 ", len(items), u" 个任务到Spider queue"


def get_level2_directory(content, level, parent, parent_url, parent_path):
    """ get level2 """
    items = Selector(text=content).xpath("//table[@class='citytable']/tr[@class='citytr']")

    for item in items:
        code = item.xpath("./td[1]/a/text()").extract()
        pn = item.xpath("./td[2]/a/text()").extract()
        leaf = False
        if len(code) == 0:
            leaf = True
            code = item.xpath("./td[1]/text()").extract()
            pn = item.xpath("./td[2]/text()").extract()

        name = code[0] + '-' + pn[0]
        # parent_url.replace(".html", "") + href[href.find('/') + 1:]

        # add url to spider_task_queue
        if not leaf:
            href = item.xpath("./td[1]/a/@href").extract()[0]
            spider_task_queue.append({
                "level": level + 1,
                "url": parent_url.replace(".html", "") + href[href.find('/'):],
                "name": name,
                "path": parent_path + "/" + name if parent_path != "" else name
            })

        result_list.append({
            "name": name,
            "level": level + 1,
            "parent": parent,
            "leaf": leaf,
            "path": parent_path + "/" + name if parent_path != "" else name
        })

        if env == DEV:
            break
    print u"\t\t|- 提交 ", len(items), u" 个任务到Spider queue"


def get_level3_directory(content, level, parent, parent_url, parent_path):
    """
    parent_url: like http://www.stats.gov.cn/tjsj/tjbz/tjypflml/2010/03/0303.html
    sub_url like http://www.stats.gov.cn/tjsj/tjbz/tjypflml/2010/03/03/030303.html
    """
    items = Selector(text=content).xpath("//table[@class='countytable']/tr[@class='countytr']")
    for item in items:
        code = item.xpath("./td[1]/a/text()").extract()
        pn = item.xpath("./td[2]/a/text()").extract()
        leaf = False
        if len(code) == 0:
            leaf = True
            code = item.xpath("./td[1]/text()").extract()
            pn = item.xpath("./td[2]/text()").extract()

        # code = item.xpath("./td[1]/a/text()").extract()[0]
        name = code[0] + '-' + pn[0]

        # add url to spider_task_queue
        if not leaf:
            href = item.xpath("./td[1]/a/@href").extract()[0]
            spider_task_queue.append({
                "level": level + 1,
                "url": parent_url[0:parent_url.rfind('/') + 1] + href,
                "name": name,
                "direct": len(code) > 6,
                "path": parent_path + "/" + name if parent_path != "" else name
            })

        result_list.append({
            "name": name,
            "level": level + 1,
            "parent": parent,
            "leaf": leaf,
            "path": parent_path + "/" + name if parent_path != "" else name
        })

        if env == DEV:
            break
    print u"\t\t|- 提交 ", len(items), u" 个任务到Spider queue"


def get_level4_directory(content, level, parent, parent_url, parent_path):
    items = Selector(text=content).xpath("//table[@class='towntable']/tr[@class='towntr']")
    for item in items:
        code = item.xpath("./td[1]/a/text()").extract()
        pn = item.xpath("./td[2]/a/text()").extract()
        leaf = False
        if len(code) == 0:
            leaf = True
            code = item.xpath("./td[1]/text()").extract()
            pn = item.xpath("./td[2]/text()").extract()

        name = code[0] + '-' + pn[0]

        # add url to spider_task_queue
        if not leaf:
            href = item.xpath("./td[1]/a/@href").extract()[0]
            spider_task_queue.append({
                "level": level + 1,
                "url": parent_url[0:parent_url.rfind('/') + 1] + href,
                "name": name,
                "path": parent_path + "/" + name if parent_path != "" else name
            })

        result_list.append({
            "name": name,
            "level": level + 1,
            "parent": parent,
            "leaf": leaf,
            "path": parent_path + "/" + name if parent_path != "" else name
        })

        if env == DEV:
            break
    print u"\t\t|- 提交 ", len(items), u" 个任务到Spider queue"


def get_level5_directory(content, level, parent, parent_path):
    items = Selector(text=content).xpath("//table[@class='villagetable']/tr[@class='villagetr']")
    for item in items:
        name = item.xpath("./td[1]/text()").extract()[0] + '-' + item.xpath("./td[2]/text()").extract()[0]

        result_list.append({
            "name": name,
            "level": level + 1,
            "parent": parent,
            "leaf": True,
            "path": parent_path + "/" + name if parent_path != "" else name
        })
    
    print u"\t\t|- 解析 ", len(items), u" 个叶子目录"


def parse_html(content, level, name, url, path):
    print u"\t|- 开始处理 get level -> ", level + 1
    if level == 0:
        get_level1_directory(content, path)

    if level == 1:
        get_level2_directory(content, level, name, url, path)

    if level == 2:
        get_level3_directory(content, level, name, url, path)

    if level == 3:
        get_level4_directory(content, level, name, url, path)

    if level == 4:
        get_level5_directory(content, level, name, path)

    print u"\t|- 处理完成 get level -> ", level + 1


def valid(path):
    path_arr = path.split("/")
    if len(path_arr) <= 5:
        return path_arr
    else:
        return None


def handle_result(file_name):
    martx = []
    max_level = 5
    for res in result_list:
        if 'leaf' in res and res['leaf']:
            path = res['path']
            items = valid(path)
            # 如果path路径大于5，就不合法，记录下来
            if items is None:
                err_list.append(res)
                continue

            row = []
            for item in items:
                row.append(item)

            # 不足5个路径的，补全
            for i in range(max_level - len(row)):
                row.append("")

            martx.append(row)
        elif 'leaf' not in res:
            err_list.append(res)

    df = pd.DataFrame(martx, columns=['一级目录', '二级目录', '三级目录', '四级目录', '五级目录'])
    df.to_excel(file_name)


def downloader(task):
    headers = {
        'User-Agent': random.choice(user_agent)
    }
    try:
        resp = requests.get(task['url'], timeout=15, headers=headers)
        print u"\t|- 请求响应 -> ", resp.status_code, resp.encoding, resp.url
        if resp.status_code != 200:
            return None

        return resp
    except ReadTimeout:
        print u"\t|- 抓取超时，再次加入 spider queue"
        return None


@click.command()
@click.argument('start', type=int)
@click.argument('end', type=int)
def main(start, end):
    """ spider """

    if env == DEBUG:
        test_handle_result()
        test_get_level2()
        test_get_level3()
        test_get_level4()
        test_get_level5()
        exit()

    print u"处理目录入口链接 -> ", start, end
    init_spider_queue(start, end)

    while len(spider_task_queue) != 0:
        task = spider_task_queue.popleft()
        print u"处理爬虫任务 - ", task

        # download
        # try:
        #     resp = requests.get(task['url'], timeout=10)
        #     print resp.status_code, resp.encoding, resp.url
        # except ReadTimeout:
        #     print "抓取超时，再次加入 spider queue"
        #     spider_task_queue.append(task)
        #     continue
        resp = downloader(task)
        if resp is None:
            spider_task_queue.append(task)
            continue

        # parse html
        if "direct" in task and task['direct']:
            print u"\t|- 特殊处理 -> ", task['url']
            get_level5_directory(resp.content, task['level'], task['name'], task['path'])

        try:
            content = resp.content.decode("gb2312")
        except UnicodeDecodeError:
            content = resp.content.decode("utf-8")

        parse_html(content, task['level'], task['name'], task['url'], task['path'])

        print u"\t|- 当前剩余 ", len(spider_task_queue), u" 个待处理任务"

        time.sleep(random.random() + 0.2)

    # save result
    res_local_name = "res-{}-{}.xlsx".format(str(start), str(end))
    print u"\t|- 保存解析结果到 ", res_local_name
    df = pd.DataFrame(result_list)
    df.to_excel(res_local_name)

    file_name = "directory-{}-{}.xlsx".format(str(start), str(end))
    handle_result(file_name)

    print err_list
    print u"爬虫任务爬取结束"


def init_spider_queue(start, end):
    """ init spider task queue """
    for i in range(start, end):
        if i == 0:
            page_num = ""
        else:
            page_num = "_" + str(i)

        spider_task_queue.append({
            "level": 0,
            "url": format_url(directory_url_tpl, page_num),
            "name": "",
            "path": ""
        })

    print u"初始化 spider task queue 完成\n"


def test_handle_result():
    result_list.append({
        "name": "0101010101",
        "level": 5,
        "parent": "0101010",
        "path": "01/0101/010101/01010101/0101010101"
    })
    handle_result("test.xlsx")


def test_get_level2():
    url = 'http://www.stats.gov.cn/tjsj/tjbz/tjypflml/2010/17.html'
    # url = 'http://www.stats.gov.cn/tjsj/tjbz/tjypflml/2010/15.html'
    resp = requests.get(url)
    print resp.encoding
    items = Selector(text=resp.content.decode("gb2312")).xpath("//table[@class='citytable']/tr[@class='citytr']")
    # print items
    for item in items:
        code = item.xpath("./td[1]/a/text()").extract()
        if len(code) == 0:
            print item.xpath("./td[1]/text()").extract()
            pass

        pn = item.xpath("./td[2]/a/text()").extract()[0]
        print isinstance(pn, unicode)
        name = code[0] + '-' + pn
        # name = name.encode("utf-8")
        href = item.xpath("./td[1]/a/@href").extract()[0]

        print name, href
    pass


def test_get_level3():
    url = 'http://www.stats.gov.cn/tjsj/tjbz/tjypflml/2010/01/0102.html'
    resp = requests.get(url)
    items = Selector(text=resp.content.decode("gb2312")).xpath("//table[@class='countytable']/tr[@class='countytr']")
    for item in items:
        print item.xpath("./td[1]/a/@href").extract()[0]
    pass


def test_get_level4():
    url = 'http://www.stats.gov.cn/tjsj/tjbz/tjypflml/2010/01/02/010201.html'
    resp = requests.get(url)
    items = Selector(text=resp.content.decode("gb2312")).xpath("//table[@class='towntable']/tr[@class='towntr']")
    for item in items:
        print item.xpath("./td[1]/a/@href").extract()[0]
    pass


def test_get_level5():
    url = 'http://www.stats.gov.cn/tjsj/tjbz/tjypflml/2010/01/12/01/01120101.html'
    resp = requests.get(url)
    items = Selector(text=resp.content.decode("gb2312")).xpath("//table[@class='villagetable']/tr[@class='villagetr']")
    for item in items:
        print item.xpath("./td[1]/text()").extract()[0], item.xpath("./td[2]/text()").extract()[0], \
            item.xpath("./td[3]/text()").extract()
    pass


if __name__ == '__main__':
    main()
