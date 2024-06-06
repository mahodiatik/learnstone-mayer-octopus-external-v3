"""
@Author: Michael Milinković
@Date: 16.09.2022.
"""

import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

from functional import seq
from bs4 import BeautifulSoup

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class UCLSpider(scrapy.Spider):

    name = 'ucl'
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University College London'
    study_level = 'Graduate'

    english_language_certificate_map = {}

    start_urls = [
        'https://www.ucl.ac.uk/prospective-students/graduate/taught-degrees/'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UCLSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        # English language requirements
        yield scrapy.Request(
            url='https://www.ucl.ac.uk/prospective-students/graduate/english-language-requirements',
            callback=self.parse_ucl_english_requirements,
            priority=10)

        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_ucl_english_requirements(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        certificates = {}

        recognised_tests = soup.find('dl')
        for tag in recognised_tests.findChildren():
            if tag.name == 'dt':
                course_name = tag.text.strip()
                certificates[course_name] = {}
            elif tag.name == 'dd':
                levels = tag.find('ul')
                # revised language requirements 01.06.2024
                if levels:
                    levels = levels.find_all('li')
                    for lvls in levels:
                        lvl = lvls.text.strip()
                        level, level_score = map(lambda x: x.strip(), lvl.split(':'))
                        certificates[course_name][level] = level_score
        self.english_language_certificate_map = certificates

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        course_list = seq(soup.select('#programme-data-content a[href]'))\
            .map(lambda x: x['href'])\
            .to_list()

        for url in course_list:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course)

    def _get_title(self, soup: BeautifulSoup) -> str:
        try:
            title = soup.find('h1').text.strip()
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, title: str) -> Optional[str]:
        try:
            qualification = title.split()[-1]
            if qualification in ['Cert', 'Dip', '(International)']:
                qualification = " ".join(title.split()[-2:])
            # degree_type = soup.find('meta',
            #                         {"name": "programme:degree_type"})
            # qualification = degree_type['content']
        except AttributeError:
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> list[str]:
        try:
            locations = [soup.select_one('svg.feather-map-pin').next_sibling.strip()]
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> str:
        try:
            description = soup.select_one('.page-intro').text.strip()
        except AttributeError:
            description = None
        return description

    def _get_tuitions(self, soup: BeautifulSoup) -> list:
        key_info_section = soup.select_one('section.prog-key-info')

        def _get_options() -> dict:
            study_mode = key_info_section.find('h5', text='Study mode')
            sibling = study_mode.next_sibling.next_sibling

            options = {}
            if sibling.name == 'select':
                for option in sibling.select('option'):
                    options[option['value']] = option.text.strip()
            else:
                text = sibling.text.strip()
                options[text.replace('-', '').lower()] = text

            return options

        options = _get_options()

        tuitions = []
        for study_mode_key, study_mode_value in options.items():
            duration_section = key_info_section.find('h5', text='Duration')
            duration = seq(duration_section.parent.select('.study-mode'))\
                .find(lambda x: study_mode_key in x['class']).text.strip()

            fee_sections = seq(key_info_section.select('h5'))\
                .filter(lambda x: 'tuition fees' in x.text.strip())\
                .to_list()

            for fee_section in fee_sections:
                student_category = seq(fee_section.parent['class']).find(lambda x: '-' not in x)

                try:
                    fee = seq(fee_section.parent.select('div')).find(lambda x: study_mode_key in x['class']).text.strip()
                except AttributeError:
                    fee = None

                tuitions.append({
                    'study_mode': study_mode_value,
                    'duration': duration,
                    'student_category': student_category,
                    'fee': fee
                })

        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> list[str]:
        try:
            start_dates = []
            key_info_section = soup.select_one('section.prog-key-info')
            programme_start_date = key_info_section.find('h5', text='Programme starts')
            programme_start_date = programme_start_date.next_sibling.next_sibling.text.strip()
            start_dates.append(programme_start_date)
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> list[str]:
        try:
            application_dates = []
            key_info_section = soup.select_one('section.prog-key-info')
            application_date = key_info_section.find('h5', text='Applications accepted')
            application_date = application_date.next_sibling.next_sibling.text.strip()
            application_date = re.sub(r"\s\s+", " ", application_date)
            application_date = application_date.replace("All applicants:", "")\
                .replace("Applications open", "")\
                .replace("Applications closed", "")\
                .strip()
            application_dates.append(application_date)
        except AttributeError:
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> str:
        try:
            entry_requirements = soup.select_one('section.prog-requirements')\
                .find('p').text.strip()
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> list[dict]:
        try:
            english_language_requirements = []
            english_language_level = soup.select_one('section.prog-requirements')\
                .select_one('dl.accordion')\
                .select_one('strong')\
                .text.strip()
            if 'Level' in english_language_level:
                level = english_language_level[:7]
                if 'TOEFL' in english_language_level:
                    english_language_requirements.append({
                        "test": 'UCL Pre-sessional English Courses',
                        "score": self.english_language_certificate_map['UCL Pre-sessional English Courses'][level]
                    })
                    english_language_requirements.append({
                        "test": 'UCL International Pre-Master\'s Courses',
                        "score": self.english_language_certificate_map['UCL International Pre-Master\'s Courses'][level]
                    })
                    english_language_requirements.append({
                        "test": 'Test of English as Foreign Language (TOEFL) iBT',
                        "score": self.english_language_certificate_map['Test of English as Foreign Language (TOEFL) iBT'][level]
                    })
                    english_language_requirements.append({
                        "test": 'International English Language Testing System (IELTS) Academic Version',
                        "score": self.english_language_certificate_map['International English Language Testing System (IELTS) Academic Version'][level]
                    })
                else:
                    for key, value in self.english_language_certificate_map.items():
                        english_language_requirements.append({
                            "test": key,
                            "score": value[level]
                        })
            else:
                english_language_requirements.append({
                            "test": 'International English Language Testing System (IELTS) Academic Version',
                            "score": english_language_level
                })

        except AttributeError:
            english_language_requirements = []
        return english_language_requirements

    def _get_about(self, soup: BeautifulSoup) -> str:
        try:
            about_section = soup.select_one('.prog-overview')
            content = []
            for tag in about_section.findChildren():
                content.append(str(tag))
            about = "".join(content)
        except AttributeError:
            about = None
        return about

    def _get_modules(self, soup: BeautifulSoup) -> list:
        try:
            modules = []
            module_section = soup.select_one('.prog-modules')
            for module_list in module_section.select('div'):
                try:
                    module_type = seq(module_list['class'])\
                        .find(lambda x: x.startswith('prog-modules'))
                    if not module_type:
                        continue
                    module_type = re.match(r'prog-modules-(.*)', module_type)\
                        .group(1)

                    for module in module_list.select('a'):
                        modules.append({
                            'type': module_type.capitalize(),
                            'title': module.text.strip(),
                            'link': module['href']
                        })
                    if module_list.select('a') == []:
                        for module in module_list.select('div'):
                            modules.append({
                                'type': module_type.capitalize(),
                                'title': module.text.strip(),
                                'link': None
                            })

                except KeyError:
                    pass
        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        title = self._get_title(soup)
        study_level = self.study_level
        qualification = self._get_qualification(title)
        university = self.university
        locations = self._get_locations(soup)
        description = self._get_description(soup)
        about = self._get_about(soup)
        tuitions = self._get_tuitions(soup)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)  # change to html
        language_requirements = self._get_english_language_requirements(soup)  # Will need to create mapper for known certificates
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
    cp.crawl(UCLSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
