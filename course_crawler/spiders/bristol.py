"""
@Author: Md Mahodi Atik Shuvo
@Date: 29-05-2024"""

import os
import re
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from string import ascii_uppercase
from typing import List, Optional, Tuple

from thefuzz import process
from functional import seq
from bs4 import BeautifulSoup, Tag

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.reactor import install_reactor
import requests


class BristolSpider(scrapy.Spider):

    name = 'bristol'
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University of Bristol'
    study_level = 'Graduate'

    unique_courses = set()

    language_certificates = {}
    tuition_fees = {}

    start_urls = ["http://www.bristol.ac.uk/study/postgraduate/search/?filterStudyType=Taught&q="]

    # Overrides configuration values defined in course_crawler/settings.py
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler"
        }
    }

    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BristolSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        yield scrapy.Request(
            url='https://bristol.ac.uk/students/support/finances/tuition-fees/pgt/home/23-24/2023-starters/',
            callback=self.parse_tuitions,
            meta={'tuition_type': 'uk'})

    def parse_tuitions(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        tuition_type = response.meta['tuition_type']
        self.tuition_fees[tuition_type] = []

        table = soup.select_one('.table-filter')

        for row in table.select('tr')[1:]:
            _, _, programme, mode, fee = row.select('td')
            self.tuition_fees[tuition_type].append(
                (programme.text, 'Part-time' if mode.text != 'FT' else 'Full-time', fee.text))

        if len(self.tuition_fees.keys()) == 2:
            yield scrapy.Request(
                url='https://www.bristol.ac.uk/study/language-requirements/',
                callback=self.parse_english_language_requirement_list)
        else:
            yield scrapy.Request(
                url='https://bristol.ac.uk/students/support/finances/tuition-fees/pgt/overseas/23-24/2023-starters/',
                callback=self.parse_tuitions,
                meta={'tuition_type': 'international'})

    def parse_english_language_requirement_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        profile_links = seq(soup.select('.list-menu a'))\
            .map(lambda x: f"https://www.bristol.ac.uk{x['href']}").to_list()[:-1]

        for profile_link in profile_links:
            yield scrapy.Request(
                url=profile_link,
                callback=self.parse_english_language_requirement)

    def parse_english_language_requirement(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        try:
            table = soup.find('h2', text='English Language Proficiency Tests').find_next_sibling()
        except:
            table = soup.find('button', text='English Language Proficiency Tests').find_next_sibling()
        requirements = []
        for row in table.select('tr')[1:]:
            test = row.select('td')[0].text.strip()
            score = row.select('td')[1].text.strip()

            requirements.append({
                'test': test,
                'score': score
            })

        profile_level = response.url.split('-')[-1].replace('/', '')

        self.language_certificates[profile_level] = requirements

        if len(self.language_certificates.keys()) == 8:
            for url in self.start_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        for course_card in seq(soup.select('.search-result--course'))\
                .filter(lambda x: 'Taught' in x.select_one('.search-result__taxonomy').text):
            link = f"https://www.bristol.ac.uk{course_card.select_one('a')['href']}"
            if "https://www.bristol.ac.uk/study/postgraduate/taught/" in link:
                qualifications = self._get_qualifications(course_card)

                for qualification in qualifications:
                    yield scrapy.Request(
                        url=link,
                        callback=self.parse_course,
                        dont_filter=True,
                        meta={
                            'qualification': qualification,
                            'qualifications': qualifications,  # used for cleaning the title,
                            'playwright': True
                        })
            else:
                continue

    def _get_qualifications(self, tag: Tag) -> list[str]:
        try:
            qualification_section = tag.select_one('.search-result__meta')
            qualifications = seq(qualification_section.select('dd')[-1].text.split(','))\
                .map(lambda x: re.sub(r'\(.*\)', '', x).strip())\
                .to_list()
        except AttributeError:
            qualifications = []
        return qualifications

    def _get_title(self, soup: BeautifulSoup, qualifications: list[str]) -> Optional[str]:
        try:
            title = soup.select_one('h1').text.strip()
            for qualification in qualifications:
                title = title.replace(qualification, '').strip()
        except AttributeError:
            title = None
        return title

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            location_section = soup.find('dt', text='Location')
            locations = seq(location_section.next_sibling.text.split(','))\
                .map(lambda x: x.strip())\
                .map(lambda x: f"{x}, Bristol" if not x.lower().startswith('distance') else x)\
                .to_list()
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description_section = soup.select_one('.course-overview__main').select_one('p').text
            description = f"{description_section.split('.')[0].strip()}."
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about_section = soup.select_one('.course-overview__main')
            about_section.select_one('h2').decompose()
            about = str(about_section)
        except AttributeError:
            about = None
        return about

    def _get_tuitions(self, soup: BeautifulSoup, title: str, qualification: str) -> list:
        try:
            tuitions = []

            programme_duration = soup.find("dt", text="Programme duration")
            durations = programme_duration.find_next_sibling().get_text(separator="|", strip=True)
            study_mode_duration_map = {}

            for duration_mode in durations.split('|'):
                if 'full-time' in duration_mode:
                    study_mode_duration_map['full-time'] = duration_mode.split('full-time')[0].strip()
                elif 'part-time' in duration_mode:
                    study_mode_duration_map['part-time'] = duration_mode.split('part-time')[0].strip()
            
            # Use fuzzy string matching to determine tuition fees match
            for student_category in self.tuition_fees.keys():
                choices = seq(self.tuition_fees[student_category])\
                    .map(lambda x: x[0])\
                    .to_list()
                match, _ = process.extractOne(f"{title} ({qualification})", choices)

                _, study_mode, fee = self.tuition_fees[student_category][choices.index(match)]

                if study_mode.lower() in study_mode_duration_map:
                    duration = study_mode_duration_map[study_mode.lower()]
                else:
                    duration = None
                tuitions.append({
                    'study_mode': study_mode,
                    'duration': duration,
                    'student_category': student_category,
                    'fee': fee
                })
        except (AttributeError, ValueError):
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_date_section = soup.find('dt', text='Start date')
            start_dates = seq(start_date_section.next_sibling
                              .get_text(separator='|', strip=True)
                              .split('|'))\
                .map(lambda x: x.strip())\
                .to_list()
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates_section = soup.find('dt', text='Application deadline').find_next_sibling()
            application_dates = re.findall(r'\d+(?:st|nd|rd|th)?\s\w+\s\d+', application_dates_section.text)
        except (AttributeError, TypeError):
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements_block = soup.find("section#entry-requirements")
            entry_requirements = entry_requirements_block.find_next().text.strip()
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []

            english_language_requirements_section = soup.select_one('#accordion-english-language')
            profile = english_language_requirements_section.select_one('a')['href'].split('-')[-1]
            profile = profile.replace('/', '')

            for requirement in self.language_certificates[profile]:
                english_language_requirements.append({
                    'language': 'English',
                    **requirement
                })
        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules_link(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            programme_structure_section = soup.select_one('#programme-structure')
            modules_links = programme_structure_section.select('a')
            modules_link = None
            for link in modules_links:
                if 'unit-programme-catalogue' in link['href']:
                    modules_link = link['href']
                    break
        except AttributeError:
            modules_link = None
        return modules_link

    def _get_modules(self, soup: BeautifulSoup) -> List[dict]:
        try:
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
            selector=soup.select("#uobcms-content  div.column.grid_8 li")
            for i in selector:
                try:
                    linx=i.select_one("a")["href"]
                    linx= f'https://www.bris.ac.uk/{linx}'
                except:
                    continue
                response= requests.get(linx)
                soup= BeautifulSoup(response.content, "html.parser")
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

                
        except (AttributeError, ValueError):
            modules = []
        return modules

    # TODO: modules for other years of study could also be retrieved
    def parse_modules(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        item = response.meta['item']

        modules = self._get_modules(soup)
        item['modules'] = modules

        if (item["link"], item["qualification"]) not in self.unique_courses:
            self.unique_courses.add((item["link"], item["qualification"]))
            yield item

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        qualification = response.meta['qualification']
        title = self._get_title(soup, response.meta['qualifications'])
        study_level = self.study_level
        university = self.university
        if title is None:
            return
        if 'online' in title.lower():
            locations = ['Online']
        else:
            locations = self._get_locations(soup)
        description = self._get_description(soup)
        about = self._get_about(soup)
        tuitions = self._get_tuitions(soup, title, qualification)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)

        item = {
            'link': link,
            'title': title,
            'study_level': study_level,
            'qualification': qualification,
            'university_title': university,
            'locations': locations,
            'description': description,
            'about': about,
            'tuitions': tuitions,
            'start_dates': start_dates,
            'application_dates': application_dates,
            'entry_requirements': entry_requirements,
            'language_requirements': language_requirements
        }

        modules_link = self._get_modules_link(soup)
        if link != 'https://www.bristol.ac.uk/study/postgraduate/':
            if modules_link:
                yield scrapy.Request(
                    url=modules_link,
                    callback=self.parse_modules,
                    dont_filter=True,
                    meta={'item': item}
                )
            else:
                item['modules'] = []

                if (link, qualification) not in self.unique_courses:
                    self.unique_courses.add((link, qualification))
                    yield item


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(BristolSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()