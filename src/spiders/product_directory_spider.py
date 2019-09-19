# -*- coding: utf-8 -*-

import requests
from scrapy.selector import Selector

# 产品目录入口地址
directory_url_tpl = "http://www.stats.gov.cn/tjsj/tjbz/tjypflml/index{}.html"
directory_page_cnt = 5


def download_page():
    for i in range(directory_page_cnt):
        if i == 0:
            page_num = ""
        else:
            page_num = "_" + str(i)

        url = directory_url_tpl.format(page_num)
        print "下载页面 " + url
        resp = requests.get(url)
        parse_primary_directory(resp.content)


def parse_primary_directory(content):
    """
    解析一级目录
    :param content: html 代码
    :return:
    """
    # print content
    hrefs = Selector(text=content)\
        .xpath("//ul[@class='center_list_contlist']/li/a/@href").extract()
    print hrefs
    directory_names = Selector(text=content)\
        .xpath("//ul[@class='center_list_contlist']/li/a/*/font[@class='cont_tit03']/text()").extract()
    print directory_names

    items = Selector(text=content) \
        .xpath("//ul[@class='center_list_contlist']/li/a")
    # print items

    for item in items:
        print item.xpath("./@href").extract()
        print item.xpath("./*/font[@class='cont_tit03']/text()").extract()

    exit()


def main():
    """
    任务分解
    1. 抓取产品一级分类并解析，返回一级分类的链接地址和分类名称
    2. 从一级分类中获取所有的二级分类，返回二级分类名称和链接地址，组成(一级分类, 二级分类)
    3. 从二级分类中获取所有的三级分类，返回
    """
    # resp = requests.get("https://baidu.com")
    # print resp.content
    download_page()


if __name__ == '__main__':
    main()
