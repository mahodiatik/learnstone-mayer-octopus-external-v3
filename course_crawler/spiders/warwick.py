"""
@Author: Md. Mahodi Atik Shuvo
@Date: 25.05.24
"""

import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

from functional import seq
from bs4 import BeautifulSoup, Tag

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class WarwickSpider(scrapy.Spider):

    name = 'warwick'
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University of Warwick'
    study_level = 'Graduate'

    english_language_certificate_map = {}
    tuition_taught_course_fees_map = {}
    research_fees = []
    default_application_dates=[]

    start_urls = [
        'https://warwick.ac.uk/study/postgraduate/courses/'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WarwickSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        # English language requirements
        yield scrapy.Request(
            url='https://warwick.ac.uk/study/postgraduate/apply/english/englishlanguagealternative/',
            callback=self.parse_warwick_english_requirements,
            meta=dict(
                            playwright=True,
                            playwright_include_page=True,
                            errback=self.errback
                            ))

        # Tuition taught course fees
        yield scrapy.Request(
            url='https://warwick.ac.uk/services/finance/studentfinance/fees/postgraduatefees',
            callback=self.parse_warwick_tuition_fees
            )
        # Research fees
        yield scrapy.Request(
            url='https://warwick.ac.uk/services/finance/studentfinance/fees/pgr',
            callback=self.parse_warwick_research_fees
            )
        
        #application dates
        yield scrapy.Request(
            url='https://warwick.ac.uk/study/postgraduate/apply/english',
            callback=self.parse_application_dates
            )

        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_warwick_tuition_fees(self, response: HtmlResponse):
        soup = BeautifulSoup(
                            response.body,
                            'html.parser',
                            from_encoding='utf-8')
        tuition_fees = {}
        tuition_fees_table = soup.find(attrs={"id": "searchable-table"}) \
                                 .find('tbody')
        for row in tuition_fees_table.select('tr'):
            cells = row.select('td')
            course_name = cells[0].text.strip()
            study_mode = cells[2].text.strip()
            student_category = cells[3].text.strip()
            fee = cells[6].text
            tuition_fee_item = {
                "study_mode": study_mode,
                "duration": '',
                "student_category": student_category,
                "fee": fee
            }
            if course_name in tuition_fees:
                tuition_fees[course_name].append(tuition_fee_item)
            else:
                tuition_fees[course_name] = [tuition_fee_item]
        self.tuition_taught_course_fees_map = tuition_fees

    def parse_warwick_research_fees(self, response: HtmlResponse):
        soup = BeautifulSoup(
                            response.body,
                            'html.parser',
                            from_encoding='utf-8')
        research_fees = []
        table= soup.select_one('table')
        for row in table.select('tr'):
            temp=row.select('td')[1].text
            if(temp.lower().find('home')!=-1 or temp.lower().find('overseas')!=-1):
                student_category=temp.strip()
                full_time_fee=row.select('td')[6].text.strip()
                part_time_fee=row.select('td')[7].text.strip()
                research_fees.append({'student_category':student_category,"duration":"1 Year","study_mode":"full-time","fee":full_time_fee}) #as tuition fees are for 1 year
                research_fees.append({'student_category':student_category,"duration":"1 Year","study_mode":"part-time","fee":part_time_fee})
        self.research_fees = research_fees

    async def parse_warwick_english_requirements(self, response: HtmlResponse):
        page = response.meta["playwright_page"]
        await page.close()
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        certificates = {}

        approved_tests = soup.select_one('h2.faq-subheading')
        ok=approved_tests.find_next_siblings('div')
        for i in ok:
            test=i.select_one('h3').text.strip()
            band_text=i.select_one('h4')
            bands = re.split(', |and ', band_text.text)[1:]
            bands = [x.strip() for x in bands]
            score = band_text.find_next('p').text.strip()
            language_requirement = {
                'test': test,
                'score': score
            }
            for band in bands:
                if band in certificates:
                    certificates[band].append(language_requirement)
                else:
                    certificates[band] = [language_requirement]

        self.english_language_certificate_map = certificates
    
    def parse_application_dates(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        application_dates = []
        ok=soup.select_one('div.column-2 strong').text
        date_pattern = re.compile(r'\d{1,2} \w+ \d{4}')
        date = date_pattern.search(ok).group(0)
        application_dates.append(date)
        self.default_application_dates = application_dates

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser',
                             from_encoding='utf-8')

        course_list = seq(soup.select('dl p a[href]'))\
            .map(lambda x: ("https://warwick.ac.uk/study/postgraduate/courses/"+x['href'], x.text))\
            .to_list()

        for url, title in course_list:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course,
                                 dont_filter=True,
                                 meta={
                                     'title': title
                                 })

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title = soup.title.text
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, soup: Tag) -> Optional[str]:
        try:
            key_infos = soup.select('div.equal-height-md div.info-content')
            qualification = key_infos[3].text.strip()
        except AttributeError:
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            key_infos = soup.select('div.equal-height-md div.info-content')
            locations = key_infos[5].text.strip()
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description = soup.find(attrs={"name": "description"})['content']
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about_paragraphs = soup.find(attrs={"id": "course-tab-1"})\
                                   .select('p')
            content = []
            for paragraph in about_paragraphs:
                # content.append(paragraph.text)
                content.append(str(paragraph))
            about = "".join(content)
        except AttributeError:
            about = None
        return about

    def _get_tuitions(self, soup: BeautifulSoup, title: str, qualification: str) -> list:
        try:
            key_infos = soup.select('div.equal-height-md div.info-content')
            durations = key_infos[2].text.strip()
            durations.replace('\n', ';')
            durations = durations.split(';')
            if len(durations) == 1 and '),' in durations:
                durations = durations[0].split(',')
            durations = [x.strip() for x in durations]
            tuitions = []
            if(qualification.lower().find("phd")!=-1 or qualification.lower().find("research")!=-1 or qualification.lower().find("mphil")!=-1 or qualification.lower().find("mres")!=-1 or qualification.lower().find("ed.d")!=-1 or qualification.lower().find("engd")!=-1):
                tuitions=self.research_fees
                return tuitions
            try:
                selector=soup.find("h3",string="Fees and funding")
                selector=selector.next_sibling.next_element.text
                fee= re.search(r'£(\d+,\d+)',selector).group(0)
                tuitions.append({'student_category':"All","duration":"1 Year","study_mode":"part-time","fee":fee}) #for this case all courses are part time and fees are mentioned as per year
                return tuitions
            except:
                pass

            
            if(qualification.lower()=="pgdip"):
                qualification="Postgraduate Diploma"

            name = title[:title.find('(')-1]
            # course = qualification + ' ' + name
            course = f'{name} ({qualification})'
            if course in self.tuition_taught_course_fees_map:
                tuitions = self.tuition_taught_course_fees_map[course]

            for duration in durations:
                matched = False
                study_mode = ''
                if 'part-time' in duration.lower():
                    study_mode = "Part Time"
                    duration = " ".join(duration.split(' ')[:-1])
                elif 'full-time' in duration.lower():
                    study_mode = 'Full Time'
                    duration = " ".join(duration.split(' ')[:-1])

                for tuition_item in tuitions:
                    if tuition_item['study_mode'] == study_mode:
                        tuition_item['duration'] = duration
                        matched = True

                if not matched:
                    tuitions.append({
                        "study_mode": study_mode,
                        "duration": duration,
                        "student_category": '',
                        "fee": ''
                    })

        except AttributeError:
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            key_infos = soup.select('div.equal-height-md div.info-content')
            start_dates = key_infos[1].text.strip()
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates = self.default_application_dates
        except AttributeError:
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            minimum_requirements_section = soup.find(
                                                "h4",
                                                string="Minimum requirements")
            content = []
            for tag in minimum_requirements_section.next_siblings:
                if tag.name == "h4":
                    break
                else:
                    content.append(str(tag))
            entry_requirements = "".join(content)

        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            band = soup.find(
                lambda tag: tag.name == "li" and "Band" in tag.text)
            band = band.text.split(' ')[1].strip()
            english_language_requirements = self.english_language_certificate_map[band]
        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules(self, soup: BeautifulSoup) -> list:
        try:
            modules = []
            core_modules_section = soup.find("h2", string="Core modules")

            if core_modules_section is not None:
                for tag in core_modules_section.next_siblings:
                    if tag.name == "p":
                        title = tag.find('strong')
                        link = ''
                        if title and title.text.strip():
                            if tag.select('a'):
                                link = tag.find('a')['href']
                            modules.append({
                                "type": "Core module",
                                "title": title.text,
                                "link": link
                            })
                    elif tag.name == "ul":
                        optional_modules = seq(tag.select('li'))\
                            .map(lambda x: x.text.strip())\
                            .to_list()
                        for module in optional_modules:
                            modules.append({
                                "type": "Core module",
                                "title": module,
                                "link": ""
                            })
                    elif tag.name == 'h3':
                        break
            optional_modules_section = soup.find(
                                    "h3",
                                    string="Optional modules")
            if optional_modules_section is not None:
                for tag in optional_modules_section.next_siblings:
                    if tag.name == "ul":
                        optional_modules = seq(tag.select('li'))\
                                            .map(lambda x: x.text.strip())\
                                            .to_list()
                        for module in optional_modules:
                            modules.append({
                                "type": "Optional module",
                                "title": module,
                                "link": ""
                            })
        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser',
                             from_encoding='utf-8')

        link = response.url
        # title = self._get_title(soup)
        title = response.meta['title']
        study_level = self.study_level
        qualifications = self._get_qualification(soup)
        university = self.university
        locations = self._get_locations(soup)
        description = self._get_description(soup)
        about = self._get_about(soup)

        # TODO improve tuition
        # some courses cannot be mapped by qualification+name
        # tuition is not mapped for 130 courses
        # 2024 entries are missing
        # research courses are not mapped
        

        # TODO add application dates
        # https://warwick.ac.uk/study/postgraduate/apply/english
        # no deadline for UK and EU residents
        # deadlines might be different among departments
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)

        # TODO improve modules
        # optional modules can vary from year to year
        # some <li> doesn't represent module name
        modules = self._get_modules(soup)
        for qualification in qualifications.split('/'):
            tuitions = self._get_tuitions(
                soup,
                title,
                qualification)
            start_dates = self._get_start_dates(soup)
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

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(WarwickSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
