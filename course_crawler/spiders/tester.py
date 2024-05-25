# from datetime import datetime
# import json
# import os
# import re
# from pathlib import Path
# from typing import Dict, List
# from urllib.parse import parse_qs
# from bs4 import BeautifulSoup, Tag
# from functional import seq
# import scrapy
# from scrapy import signals
# from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings
# from scrapy.http import HtmlResponse
# from scrapy_playwright.page import PageMethod
# from playwright.sync_api import sync_playwright, Playwright
# import requests
# import asyncio
# from playwright.async_api import async_playwright, Playwright

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

# '''

# # english_language_requirements = ""
# # selector = soup.select("div#entry-requirements p")
# # for i in selector:
# #     next=i.find_next("h3")
# #     if(next.text=="English language requirements"):
# #         english_language_requirements+=str(i)
# #     else:
# #         break
# # print(english_language_requirements)
# url= "https://warwick.ac.uk/study/postgraduate/courses/msc-psychotherapy-counselling"
# response= requests.get(url)
# soup= BeautifulSoup(response.content, 'html.parser')

# def _get_tuitions(soup: BeautifulSoup, title: str, qualification: str) -> list:
#         try:
#             key_infos = soup.select('div.equal-height-md div.info-content')
#             durations = key_infos[2].text.strip()
#             durations.replace('\n', ';')
#             durations = durations.split(';')
#             if len(durations) == 1 and '),' in durations:
#                 durations = durations[0].split(',')
#             durations = [x.strip() for x in durations]
#             tuitions = []
#             if(qualification.lower().find("phd")!=-1 or qualification.lower().find("research")!=-1 or qualification.lower().find("mphil")!=-1 or qualification.lower().find("mres")!=-1 or qualification.lower().find("ed.d")!=-1 or qualification.lower().find("engd")!=-1):
#                 # tuitions=self.research_fees
#                 return tuitions
#             try:
#                 selector=soup.find("h3",string="Fees and funding")
#                 selector=selector.next_sibling.next_element.text
#                 fee= re.search(r'£(\d+,\d+)',selector).group(0)
#                 tuitions.append({'student_category':"All","duration":"1 Year","study_mode":"part-time","fee":fee}) #for this case all courses are part time and fees are mentioned as per year
#                 return tuitions
#             except:
#                 pass

            
#             if(qualification.lower()=="pgdip"):
#                 qualification="Postgraduate Diploma"

#             name = title[:title.find('(')-1]
#             # course = qualification + ' ' + name
#             course = f'{name} ({qualification})'
#             # if course in self.tuition_taught_course_fees_map:
#             #     tuitions = self.tuition_taught_course_fees_map[course]

#             for duration in durations:
#                 matched = False
#                 study_mode = ''
#                 if 'part-time' in duration.lower():
#                     study_mode = "Part Time"
#                     duration = " ".join(duration.split(' ')[:-1])
#                 elif 'full-time' in duration.lower():
#                     study_mode = 'Full Time'
#                     duration = " ".join(duration.split(' ')[:-1])

#                 for tuition_item in tuitions:
#                     if tuition_item['study_mode'] == study_mode:
#                         tuition_item['duration'] = duration
#                         matched = True

#                 if not matched:
#                     tuitions.append({
#                         "study_mode": study_mode,
#                         "duration": duration,
#                         "student_category": '',
#                         "fee": ''
#                     })

#         except AttributeError:
#             tuitions = []
#         return tuitions
# ok= _get_tuitions(soup,"MSc Advanced Clinical Practice (Advanced Clinical Practice)","MSc")
# print(ok)
