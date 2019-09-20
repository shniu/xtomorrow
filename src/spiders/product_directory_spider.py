# -*- coding: utf-8 -*-

import collections
import requests
import time
import pandas as pd
import numpy as np
from scrapy.selector import Selector

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


def parse_html(content, level, name, url, path):
    print "开始处理 get level -> ", level + 1
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

    print "处理完成 get level -> ", level + 1


def handle_result():
    martx = []
    max_level = 5
    for res in result_list:
        if 'leaf' in res and res['leaf']:
            path = res['path']
            items = path.split('/')

            row = []
            for item in items:
                row.append(item)

            for i in range(max_level - len(row)):
                row.append("")

            martx.append(row)

    df = pd.DataFrame(martx, columns=['一级目录', '二级目录', '三级目录', '四级目录', '五级目录'])
    df.to_excel("directory.xlsx")


def main():
    """ spider """

    if env == DEBUG:
        test_handle_result()
        test_get_level2()
        test_get_level3()
        test_get_level4()
        test_get_level5()
        exit()

    init_spider_queue()

    while len(spider_task_queue) != 0:
        task = spider_task_queue.popleft()
        print "处理爬虫任务 - ", task

        # download
        resp = requests.get(task['url'])
        print resp.status_code, resp.encoding, resp.url

        # parse html
        if "direct" in task and task['direct']:
            print "特殊处理 -> ", task['url']
            get_level5_directory(resp.content, task['level'], task['name'], task['path'])

        try:
            content = resp.content.decode("gb2312")
        except UnicodeDecodeError:
            content = resp.content.decode("utf-8")

        parse_html(content, task['level'], task['name'], task['url'], task['path'])

        time.sleep(3)

    print result_list
    handle_result()


def init_spider_queue():
    """ init spider task queue """
    for i in range(directory_page_cnt):
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
        break
    print "初始化 spider task queue 完成\n"


def test_handle_result():
    result_list.append({
        "name": "0101010101",
        "level": 5,
        "parent": "0101010",
        "path": "01/0101/010101/01010101/0101010101"
    })
    handle_result()


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
