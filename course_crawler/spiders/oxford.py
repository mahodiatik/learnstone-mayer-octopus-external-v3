"""
@Author: Michael Milinković
@Date: 01.04.2023.
"""

import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from string import ascii_uppercase
from typing import List, Optional, Tuple

from functional import seq
from bs4 import BeautifulSoup, Tag

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class OxfordSpider(scrapy.Spider):

    name = 'oxford'
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University of Oxford'
    study_level = 'Graduate'

    language_certificates = {}

    start_urls = ["https://www.ox.ac.uk/admissions/graduate/courses/courses-a-z-listing?page=0",
                    "https://www.ox.ac.uk/admissions/graduate/courses/courses-a-z-listing?page=1",
                    "https://www.ox.ac.uk/admissions/graduate/courses/courses-a-z-listing?page=2",
                    "https://www.ox.ac.uk/admissions/graduate/courses/courses-a-z-listing?page=3",
                  "https://www.ox.ac.uk/admissions/graduate/courses/courses-a-z-listing?page=4"]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(OxfordSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        yield scrapy.Request(url="https://www.ox.ac.uk/admissions/graduate/applying-to-oxford/application-guide/qualifications-experience-languages-funding/english-language-proficiency",
                             callback=self.parse_english_language_requirements)

    def parse_english_language_requirements(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        def _parse_table(table: Tag) -> dict:
            overall_caption = table.select('th')[1].text.strip()
            per_component_caption = table.select('th')[2].text.strip()

            language_tests = {}
            for row in table.select('tbody tr'):
                test = re.sub(r'\(.*\)', '', row.select('td')[0].text)
                test = test.replace('*', '').replace('†', '').strip()

                overall_score = row.select('td')[1].text.strip()
                per_component_score = str(row.select('td')[2]).replace('<br/>', ' ')
                per_component_score = BeautifulSoup(per_component_score, 'html.parser', from_encoding='utf-8').text.strip()
                score = "%s: %s, %s: %s" % (overall_caption, overall_score, per_component_caption, per_component_score)

                language_tests[test] = score
            return language_tests

        self.language_certificates['Standard'] = _parse_table(soup.select('table')[0])
        self.language_certificates['Higher'] = _parse_table(soup.select('table')[0])

        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        for course_card in soup.select('.course-listing'):
            link = f"https://www.ox.ac.uk{course_card.select_one('a')['href']}"
            title = course_card.select_one('a').text.strip()

            course_card.select_one('a').decompose()

            qualification = course_card.select_one('.course-title').text.strip()
            location = f"{course_card.select_one('.course-department').text.strip()}, Oxford"
            study_mode = course_card.select_one('.course-mode-of-study').text.strip()
            duration = course_card.select_one('.course-duration').text.strip()

            yield scrapy.Request(url=link,
                                 callback=self.parse_course,
                                 dont_filter=True,
                                 meta={
                                     'title': title,
                                     'qualification': qualification,
                                     'location': location,
                                     'study_mode': study_mode,
                                     'duration': duration
                                 })

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description_section = soup.select_one('.field-name-field-intro')
            description_section.select_one('h2').decompose()
            description = description_section.text.strip()
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about_tab_idx = seq(soup.select('.field_tab_title li')) \
                .map(lambda x: x.text.strip().lower()) \
                .to_list() \
                .index('about')

            about_section = soup.select_one('div[about]')\
                .select_one('.field-name-field-body-multiple')\
                .select('.field-item')[about_tab_idx]

            about = str(about_section)
        except AttributeError:
            about = None
        return about

    def _get_tuitions(self, soup: BeautifulSoup, study_mode: str, duration: str) -> list:
        try:
            tuitions = []

            tuition_tab_idx = seq(soup.select('.field_tab_title li'))\
                .map(lambda x: x.text.strip().lower())\
                .to_list()\
                .index('funding and costs')

            tuition_section = soup.select_one('div[about]') \
                .select_one('.field-name-field-body-multiple') \
                .select('.field-item')[tuition_tab_idx]

            tables = tuition_section.select('#feetable')
            table_idx = 1 if study_mode == 'Part time' and len(tables) > 1 else 0
            table = tables[table_idx]

            for student_category, student_category_idx in [('uk', 1), ('international', 2)]:
                tuitions.append({
                    'study_mode': study_mode,
                    'duration': duration,
                    'student_category': student_category,
                    'fee': table.select('tr')[student_category_idx].select('td')[1].text.strip()
                })
        except (AttributeError, ValueError):
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates = []
            start_date = soup.select_one('#coursestart td').text.strip()
            if re.search(r'\d', start_date):
                start_dates.append(start_date)
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_date_section = seq(soup.select_one('#page-content-sidebar-second').select('div'))\
                .find(lambda x: 'Deadlines' in str(x))
            application_dates = seq(application_date_section.select('strong'))\
                .filter(lambda x: re.search(r'\d{4}', x.text))\
                .map(lambda x: x.text.strip())\
                .to_list()
        except (AttributeError, TypeError):
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements_tab_idx = seq(soup.select('.field_tab_title li')) \
                .map(lambda x: x.text.strip().lower()) \
                .to_list() \
                .index('entry requirements')

            entry_requirements_section = soup.select_one('div[about]') \
                .select_one('.field-name-field-body-multiple') \
                .select('.field-item')[entry_requirements_tab_idx]
            entry_requirements = entry_requirements_section.select_one('strong').find_parent().text.strip().capitalize()
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []

            english_language_level = soup.select_one('#courselangreq td').text.split()[0].strip()

            for test, score in self.language_certificates[english_language_level].items():
                english_language_requirements.append({
                    'language': 'English',
                    'test': test,
                    'score': score
                })
        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules(self, soup: BeautifulSoup) -> List[dict]:
        try:
            modules= []
            try:
                module_selector=soup.select("#content-tab ul li")
                for i in module_selector:
                    if "core" in i.find_previous('p').text.lower():
                        modules.append({"title":i.text,"type":"Core","link":""})
                    elif "option" in i.find_previous('p').text.lower():
                        modules.append({"title":i.text,"type":"Optional","link":""})
            except:
                modules = []

        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        tabs = seq(soup.select('.field_tab_title li')).map(lambda x: x.text.strip()).to_set()
        if not {'About', 'Entry requirements', 'Funding and Costs'}.issubset(tabs):
            return

        link = response.url
        title = response.meta['title']
        study_level = self.study_level
        qualification = response.meta['qualification']
        university = self.university
        locations = [response.meta['location']]
        description = self._get_description(soup)
        about = self._get_about(soup)
        tuitions = self._get_tuitions(soup, response.meta['study_mode'], response.meta['duration'])
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)
        modules = self._get_modules(soup)

        yield {
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
            'language_requirements': language_requirements,
            'modules': modules
        }


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(OxfordSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
