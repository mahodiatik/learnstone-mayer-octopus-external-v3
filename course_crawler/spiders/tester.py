from datetime import datetime
import json
import os
import re
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs
from bs4 import BeautifulSoup, Tag
from functional import seq
import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.http import HtmlResponse
from scrapy_playwright.page import PageMethod
from playwright.sync_api import sync_playwright, Playwright
import requests
import asyncio
from playwright.async_api import async_playwright, Playwright

# '''
# #playwright section----use if needed
# async def call():
#     playwright = await async_playwright().start()
#     browser = await playwright.chromium.launch(headless = True)
#     page = await browser.new_page()

#     await page.goto("https://www.sheffield.ac.uk/postgraduate/taught/courses/2024/advanced-clinical-practice-gp-mmedsci")
#     content = await page.content()

#     await browser.close()
#     await playwright.stop()
#     return content
# content=asyncio.run(call())
# soup = BeautifulSoup(content, "html.parser")

link= "https://www.bris.ac.uk/unit-programme-catalogue/RouteStructure.jsa?ayrCode=24%2F25&byCohort=N&programmeCode=2GEOG011T"
response= requests.get(link)
soup= BeautifulSoup(response.content, "html.parser")

modules = []
table = soup.select_one('.table-basic')
table.select_one('thead').decompose()

for row in table.select('tr'):
    tds = row.select('td')
    if not tds[0].select_one('a'):
        continue

    unit_name, _, _, status, _ = tds

    link = f"https://www.bristol.ac.uk{unit_name.select_one('a')['href']}"
    title = unit_name.select_one('a').text.strip()

    modules.append({
        'type': status.text.strip(),
        'title': title,
        'link': link
    })
print(len(modules))
ok=soup.select("#uobcms-content  div.column.grid_8 li")
for i in ok:
    try:
        linx=i.select_one("a")["href"]
        linx= f'https://www.bris.ac.uk/{linx}'
        print(linx)
    except:
        continue
    response= requests.get(linx)
    soup= BeautifulSoup(response.content, "html.parser")
    # print(soup)
    # table.select_one('thead').decompose()

    for row in table.select('tr'):
        tds = row.select('td')
        if not tds[0].select_one('a'):
            continue

        unit_name, _, _, status, _ = tds

        link = f"https://www.bristol.ac.uk{unit_name.select_one('a')['href']}"
        title = unit_name.select_one('a').text.strip()

        modules.append({
            'type': status.text.strip(),
            'title': title,
            'link': link
        })
print(len(modules))