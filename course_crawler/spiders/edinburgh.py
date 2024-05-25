"""
@Author: Michael Milinković
@Date: 17.11.2022.
"""

import os
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple

from functional import seq
from bs4 import BeautifulSoup, Tag

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class EdinburghSpider(scrapy.Spider):

    name = 'edinburgh'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University of Edinburgh'
    study_level = 'Graduate'

    start_urls = [
        'https://www.ed.ac.uk/studying/postgraduate/degrees/index.php?r=site/taught&edition=2023'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(EdinburghSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def call_next(self, response: HtmlResponse):
        meta = response.request.meta

        if len(meta['callstack']) > 0:
            target = meta['callstack'].pop(0)
            yield scrapy.Request(target['url'], meta=meta | target['meta'], callback=target['callback'], errback=self.call_next)
        else:
            yield meta['course']

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        course_list = seq(soup.select("a.list-group-item"))\
            .map(lambda x: (x.text.strip(), f"https://www.ed.ac.uk{x['href']}"))\
            .to_list()

        for title, url in course_list:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course,
                                 dont_filter=True)

    def _get_title(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            title_section = soup.select_one('h1.page-header')

            if title_section.select_one('small'):
                title_section.select_one('small').decompose()

            title = title_section.text.strip()
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            key_facts = soup.select_one("div[id='proxy_keyFacts']").select_one("div[class*='leftContent']").text
            qualification = key_facts.split("\n")[1].replace("Awards: ", "").strip()
        except AttributeError:
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = ['Old College, South Bridge, Edinburgh']
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description_section = soup.select_one("div[id='proxy_collapseprogramme']")
            description = description_section.select('p')[0].text.strip()
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about_section = soup.select_one("div[id='proxy_collapseprogramme']").select_one('.panel-body')
            about = str(about_section)
        except AttributeError:
            about = None
        return about

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates = []

            forms = soup.select("select[name='code2']")
            for form in forms:
                options = form.select('option')
                for option in options:
                    value = option["value"]
                    if value != "":
                        start_dates.append(option.text)
            start_dates = list(dict.fromkeys(start_dates))
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates = []

            index = 0
            application_table = soup.select_one("div[id='proxy_collapseDeadlines']").select_one('table')
            for cell in application_table("tr")[0]("th"):
                if "Application deadline" in cell.text or "Application Deadline" in cell.text:
                    break
                index += 1

            for row in application_table("tr")[1:]:
                application_dates.append(row("td")[index].text)
        except (AttributeError, TypeError):
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements = []
            entry_requirements_section = soup.select_one('#proxy_collapseentry_req').select_one('.panel-body')

            academic_requirements = seq(entry_requirements_section.find_all())\
                .find(lambda x: x.text.strip() == "Academic requirements")
            if academic_requirements:
                curr_tag = academic_requirements.next_sibling
                while curr_tag.name != 'h4':
                    if curr_tag.text.strip():
                        entry_requirements.append(str(curr_tag))
                    curr_tag = curr_tag.next_sibling
            else:
                for tag in entry_requirements_section.find_all():
                    if tag.name.startswith('h'):
                        break
                    entry_requirements.append(str(tag))

            entry_requirements = "".join(entry_requirements)
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []
            entry_requirements_section = soup.select_one('#proxy_collapseentry_req').select_one('.panel-body')

            language_requirements_section = seq(entry_requirements_section.find_all())\
                .find(lambda x: x.text.strip() == "English language tests")

            curr_tag = language_requirements_section
            while curr_tag.name != 'ul':
                curr_tag = curr_tag.next_sibling

            for language_test in curr_tag.select('li'):
                test, score = language_test.text.strip().split(':')
                english_language_requirements.append({
                    'language': 'English',
                    'test': test,
                    'score': score
                })

        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def _get_module_link(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            module_link = soup.select_one("div[id='proxy_collapsehow_taught']").select_one('table').select_one('a')['href']
        except (AttributeError, TypeError):
            module_link = None
        return module_link

    def _get_tuition_links(self, soup: BeautifulSoup) -> list:
        try:
            tuition_links = []
            table_titles = []

            tuition_section = soup.select_one("div[id='proxy_collapsefees_and_costs']")
            tuition_table = tuition_section.select_one('table')

            for cell in tuition_table("tr")[0]("th"):
                table_titles.append(cell.text)

            for row in tuition_table("tr")[1:]:
                duration = row("td")[table_titles.index("Duration")].text
                study_mode = row("td")[table_titles.index("Study mode")].text
                link = row("td")[-1]("a")[0]['href'].replace("http://", 'https://')
                tuition_links.append({"study_mode": study_mode, "duration": duration, "link": link})
        except (AttributeError, TypeError):
            tuition_links = []
        return tuition_links

    def parse_modules(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        current_url_list = response.url.split("/")[0:-1]
        container = soup.select_one("div[class='dpt-container']")

        container_items = container.find_all(recursive=False)
        module_list = []
        type = ""
        collect = False
        for item in container_items:
            if item.name == "h4":
                if "Compulsory" in item.text:
                    type = "Mandatory"
                    collect = True
                    continue

                elif "options" in item.text:
                    type = "Optional"
                    collect = True
                    continue

            if collect:
                modules = item.findAll('div', {'class': 'dpt-course-card dpt-flex__item dpt-flex'})
                for module in modules:
                    title = module.find('a').text
                    link = "/".join(current_url_list + [module.find('a')['href']])
                    module_list.append({"type": type, "title": title, "link": link})
                collect = False

        response.meta['course']['modules'] = module_list
        return self.call_next(response)

    def parse_tuition(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        tuition_list = []
        table_title = []
        index_list = []
        i = 0

        try:
            fee_table = soup.select_one('table')

            for cell in fee_table("tr")[0]("th"):
                table_title.append(cell.text)
                if cell.text != "Academic Session"\
                        and cell.text != "Online Distance Learning"\
                        and cell.text != "Additional Programme Costs":
                    index_list.append(i)
                i += 1

            if len(fee_table("tr")) > 1:
                data_list = fee_table("tr")[1]("td")
                for index in index_list:
                    tuition_list.append({
                        "study_mode": response.meta['study_mode'],
                        "duration": response.meta['duration'],
                        "student_category": table_title[index],
                        "fee": data_list[index].text.strip(),
                    })

        except (AttributeError, TypeError):
            tuition_list = []

        if 'tuitions' not in response.meta['course']:
            response.meta['course']['tuitions'] = tuition_list
        else:
            response.meta['course']['tuitions'] += tuition_list

        return self.call_next(response)

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        title = self._get_title(soup)
        study_level = self.study_level
        qualification = self._get_qualification(soup)
        university = self.university
        locations = self._get_locations(soup)  # TODO: location available, but not structured
        description = self._get_description(soup)
        about = self._get_about(soup)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)

        course = {
            'link': link,
            'title': title,
            'study_level': study_level,
            'qualification': qualification,
            'university_title': university,
            'locations': locations,
            'description': description,
            'about': about,
            'start_dates': start_dates,
            'application_dates': application_dates,
            'entry_requirements': entry_requirements,
            'language_requirements': language_requirements,
        }

        module_link = self._get_module_link(soup)
        tuition_links = self._get_tuition_links(soup)

        callstack = []

        if module_link:
            callstack.append({
                'url': module_link,
                'callback': self.parse_modules,
                'meta': {}
            })
        else:
            course['modules'] = []

        if tuition_links:
            for tuition_link in tuition_links:
                callstack.append({
                    'url': tuition_link['link'],
                    'callback': self.parse_tuition,
                    'meta': {'study_mode': tuition_link['study_mode'], 'duration': tuition_link['duration']}
                })
        else:
            course['tuitions'] = []

        response.meta['course'] = course
        response.meta['callstack'] = callstack

        return self.call_next(response)


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(EdinburghSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
