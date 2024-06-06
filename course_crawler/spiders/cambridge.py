"""
@Author: John Doe
@Date: 01.01.2023.
"""
import re
import os
import sys
import json
import requests
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
BASEURL = 'https://www.postgraduate.study.cam.ac.uk'

class CambridgeSpider(scrapy.Spider):
    name = 'cambridge'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    university = 'University of Cambridge'
    study_level = 'Graduate'
    entry_req = ""
    start_urls = [
        "https://2024.gaobase.admin.cam.ac.uk/api/courses.datatable?taught_research=taught"
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CambridgeSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        scrapy.Request(url='https://www.postgraduate.study.cam.ac.uk/application-process/entry-requirements', callback=self._get_entry_req)
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_course_list)
    
    def extract_text(self, soup, class_name):
        container = soup.find('div', class_=class_name)
        return container.get_text(strip=True) if container else None

    def extract_text_by_keyword(self, containers, keyword):
        for container in containers:
            if keyword.lower() in container.get_text(strip=True).lower():
                return container.find_next_sibling('div', class_='campl-content-container').get_text(strip=True)
        return None
    def _get_entry_req(self, response:HtmlResponse):
        soup = BeautifulSoup(response.body, 'lxml')
        h3 = soup.find('h3', string="Academic")
        p = h3.find_next_sibling()
        while p.name != 'h3':
            self.entry_req += p.text.strip()
            p = p.find_next_sibling()
    def parse_course_list(self, response: HtmlResponse):
        data = json.loads(response.text)
        dt = data.get('data', [])
        with open('output.json', 'w') as f:
            json.dump(dt, f, indent=4)
        courses = []

        for dt in data.get('data', []):
            course = {
                'code': dt.get('code'),
                'title': dt.get('full_name'),
                'part_time': dt.get('part_time'),
                'full_time': dt.get('full_time'),
                'qualification': dt.get('qualification'),
                'prospectus_url': dt.get('prospectus_url')
            }
            courses.append(course)

        courses.sort(key=lambda x: x['prospectus_url'])
        for course in courses:
            prospectus_url = course['prospectus_url']
            if 'ice.cam.ac.uk' in prospectus_url:
                yield scrapy.Request(url=prospectus_url, 
                                     callback=self.parse_ice_course, 
                                     meta={
                                        'code':course['code'],
                                        'title': course['title'],
                                        'full_time':course['full_time'],
                                        'part_time': course['part_time'],
                                        'qualification':course['qualification']},dont_filter=True)
            elif 'postgraduate.study.cam.ac.uk'in prospectus_url:
                yield scrapy.Request(url=prospectus_url, 
                                     callback=self.parse_pg_course,
                                     meta={
                                        'code':course['code'],
                                        'title': course['title'],
                                        'full_time':course['full_time'],
                                        'part_time': course['part_time'],
                                        'qualification':course['qualification']},dont_filter=True)
            elif 'jbs.cam.ac.uk' in prospectus_url:
                yield scrapy.Request(url=prospectus_url, 
                                     callback=self.parse_jbs_course,
                                     meta={
                                        'code':course['code'],
                                        'title': course['title'],
                                        'full_time':course['full_time'],
                                        'part_time': course['part_time'],
                                        'qualification':course['qualification']},dont_filter=True)
            else:
                yield {
                    'link': course['prospectus_url'],
                    'title': course['title'],
                    'study_level': 'Graduate',
                    'description': "",
                    'university_title': 'University of Cambridge',
                    'locations': [],
                    'qualification': course['qualification'],
                    'modules': [],
                    'tuitions': [],
                    'application_dates': [],
                    'start_dates': [],
                    'about': "",
                    'entry_requirements': "",
                    'language_requirements': []
                }
    def _get_ice_description(self, soup):
        try: 
            description = self.extract_text(soup, 'field field-name-field-short-description field-type-text-long field-label-hidden ice-right-padding ice-standfirst')
        except AttributeError:
            description = ""
        return description
    def _get_ice_locations(self, soup):
        try:
            all_containers = soup.find_all('div', class_='campl-content-container campl-no-bottom-padding')
            location = self.extract_text_by_keyword(all_containers, 'Venue')
            locations = location.split('\n')
        except AttributeError:
            locations = []
        return locations

    def _get_ice_qualification(self, soup):
        try:
            all_containers = soup.find_all('div', class_='campl-content-container campl-no-bottom-padding')
            return self.extract_text_by_keyword(all_containers, 'qualification')
        except AttributeError:
            return "" 

    def _get_ice_fee(self, soup, full_time, part_time):
        try:
            cat = True
            all_containers = soup.find_all('div', class_='campl-content-container campl-no-bottom-padding')
            p = self.extract_text_by_keyword(all_containers, 'fee')
            if 'overseas' in p.lower():
                home, overseas = p.strip().split('Overseas:')
                home = home.strip().split(':')[-1]
            else:
                fee = p
                cat = False
            fees = []
            if full_time != "":
                fees.append({
                    "study_mode":"Full-time",
                    "duration":full_time,
                    "student_category":"international"if(cat)else"",
                    "fee":overseas if (cat) else fee
                    })
                if cat:
                    fees.append({
                        "study_mode":"Full-time",
                        "duration":full_time,
                        "student_category":"international",
                        "fee":home
                    })
            if part_time != "":
                fees.append({
                    "study_mode":"Part-time",
                    "duration":part_time,
                    "student_category":"international"if(cat)else"",
                    "fee":overseas if (cat) else fee
                    })
                if cat:
                    fees.append({
                        "study_mode":"Part-time",
                        "duration":part_time,
                        "student_category":"international",
                        "fee":home
                    })
            return fees
        except AttributeError:
            return []  
 

    def _get_ice_application_dates(self, soup):
        try:
            all_containers = soup.find_all('div', class_='campl-content-container campl-no-bottom-padding')
            date =  self.extract_text_by_keyword(all_containers, 'Apply by')
            dates = date.split(',')
            return dates
        except AttributeError:
            return ""  

    def _get_ice_start_date(self, soup):
        try:
            all_containers = soup.find_all('div', class_='campl-content-container campl-no-bottom-padding')
            date = self.extract_text_by_keyword(all_containers, 'dates')
            dates = date.split(',')
            return dates
        except AttributeError:
            return ""  
        
    def _parse_lang_req(self, soup):
        lang_reqs = []
        try:
            h2 = soup.find('h2', text="Language requirement")
            ul = h2.find_next_sibling('ul')
            lis = ul.find_all('li')
            if ul:
                lis = ul.find_all('li')
            else:
                lis = None
                sibs = h2.find_next_siblings('p')
                for sib in sibs:
                    if 'ielts' in sib.text.lower():
                        lis = sib
                        break
                if lis:
                    lis_str = str(lis)
                    lis_str = lis_str.replace('<br>', '\n')
                    lis = BeautifulSoup(lis_str, 'lxml').find('p')
                    text = lis.get_text()
                    lis = [item.strip().replace('•', '') for item in text.split('\n') if item.strip()]
            for li in lis:
                    test, score = li.text.split(':')
                    lang_reqs.append({
                        'language': 'English',
                        'test': test,
                        'score': score
                    })
            return lang_reqs
        except:
            lang_reqs = []

        return lang_reqs

    def parse_ice_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'lxml')

        description = self._get_ice_description(soup)
        locations = self._get_ice_locations(soup)
        fee = self._get_ice_fee(soup, response.meta['full_time'], response.meta['part_time'])
        application_dates = self._get_ice_application_dates(soup)
        start_dates = self._get_ice_start_date(soup)
        fieldsets = soup.find_all('fieldset')
        about = ""
        entry_req = ""
        lang_req = []

        for fieldset in fieldsets:
            legend = fieldset.find('span', class_='fieldset-legend')
            if legend and legend.text not in ["Fees and funding", "Entry requirements"]:
                about += fieldset.find('div', class_='field-item even').get_text(strip=True)
            if 'entry requirements' or 'requirements' in legend.text.lower():
                text_el = fieldset.find('div', class_='field-item even').children
                for t_el in text_el:
                    if 'language requirement' in t_el.text.lower():
                        pass
                    elif t_el.name != 'ul':
                        entry_req += t_el.get_text(strip=True)
        language_requirements = self._parse_lang_req(soup)
        yield {
            'link': response.url,
            'title': response.meta['title'],
            'study_level': 'Graduate',
            'description': description,
            'university_title': 'University of Cambridge',
            'locations': locations,
            'qualification': response.meta['qualification'],
            'modules': [],
            'tuitions': fee,
            'application_dates': application_dates,
            'start_dates': start_dates,
            'about': about,
            'entry_requirements': entry_req,
            'language_requirements': language_requirements
        }


    def _get_pg_application_dates(self, soup):
        try:
            sidebar = soup.select_one('#block-fieldblock-node-gao-course-default-field-gao-sidebar')
            presentations = sidebar.find('div', id='presentations')
            titles = presentations.find_all('h4', class_='panel-title')
            bodies = presentations.find_all('div', class_='panel-body')

            application_dates = []
            for idx, title in enumerate(titles):
                if 'funding' not in title.text.lower():
                    dts = bodies[idx].find_all('dt')
                    dds = bodies[idx].find_all('dd')
                    for i, dt in enumerate(dts):
                        if 'deadline' in dt.text.lower():
                            application_dates.append(dds[i].text)
            return application_dates
        except AttributeError:
            return []

    def _get_pg_start_dates(self, soup):
        try:
            sidebar = soup.select_one('#block-fieldblock-node-gao-course-default-field-gao-sidebar')
            presentations = sidebar.find('div', id='presentations')
            titles = presentations.find_all('h4', class_='panel-title')
            bodies = presentations.find_all('div', class_='panel-body')

            start_dates = []
            for idx, title in enumerate(titles):
                if 'funding' not in title.text.lower():
                    dts = bodies[idx].find_all('dt')
                    dds = bodies[idx].find_all('dd')
                    for i, dt in enumerate(dts):
                        if 'starts' in dt.text.lower():
                            start_dates.append(dds[i].text)
            return start_dates
        except AttributeError:
            return []

    def _get_pg_description(self, soup):
        try:
            desc = soup.find('div', class_='field field-name-field-gao-course-overview field-type-text-long field-label-hidden')
            return desc.find('p').text.strip() if desc else ""
        except AttributeError:
            return ""

    def _get_pg_entry_requirements(self, soup):
        entry_req = self.entry_req
        try:
            nav_ul = soup.find('ul', class_="campl-nav campl-nav-tabs campl-unstyled-list")
            li = nav_ul.find('li', string='Requirements')
            entry_req_url = BASEURL + li.a['href']
            req1 = requests.get(entry_req_url)
            soup = BeautifulSoup(req1.content, 'lxml')
            h1 = soup.find('h1', string='Expected Academic Standard')
            p = h1.find_next_sibling()
            while p.name == 'p':
                entry_req += p.text.strip()
                p = p.find_next_sibling()
        except AttributeError:
            entry_req = ""
        return entry_req

    def _get_pg_language_requirements(self, soup):
        language_requirements = []
        
        try:
            nav_ul = soup.find('ul', class_="campl-nav campl-nav-tabs campl-unstyled-list")
            li = nav_ul.find('li', string='Requirements')
            entry_req_url = BASEURL + li.a['href']
            req1 = requests.get(entry_req_url)
            soup = BeautifulSoup(req1.content, 'lxml')
            lang_req={}

            main_container = soup.find('div', class_="field field-name-field-gao-course-requirements field-type-text-long field-label-hidden")
            h3s = main_container.find_all('h3')
            for h3 in h3s:
                p = h3.find_next_sibling()
                lang_req[h3.text.strip()] = p.text.strip().replace('\n\n', '').replace('\n', ' ')
            for test in lang_req.keys():
                language_requirements.append({
                    'language': 'English',
                    'test': test,
                    'score': lang_req[test].split(':')[-1]
                })
        except AttributeError:
            language_requirements = []
        return language_requirements

    def _get_pg_fee(self, course_code, full_time, part_time):
        tuitions = []
        try:
            statuses = ['H', 'O', 'R']
            if full_time != "":
                for status in statuses:
                    feeurl = f'https://2024.gaobase.admin.cam.ac.uk/api/courses/{course_code}/financial_tracker.html?fee_status={status}&children=0'
                    resp = requests.get(feeurl)
                    sp = BeautifulSoup(resp.content, 'lxml')
                    if status == 'H':
                        cat = 'uk'
                    elif status == 'O':
                        cat = 'international'
                    else:
                        cat == 'refugee'
                    fee = sp.select_one("#fee_1 > table > tfoot > tr > th:nth-child(2)").text.strip()
                    tuitions.append({
                        "study_mode":"Full-time",
                        "duration":full_time,
                        "student_category":cat,
                        "fee":fee
                    })
            if part_time != "":
                for status in statuses:
                    feeurl = f'https://2024.gaobase.admin.cam.ac.uk/api/courses/{course_code}/financial_tracker.html?fee_status={status}&part_time=on&children=0'
                    resp = requests.get(feeurl)
                    sp = BeautifulSoup(resp.content, 'lxml')
                    if status == 'H':
                        cat = 'Home'
                    elif status == 'O':
                        cat = 'international'
                    else:
                        cat == 'refugee'
                    fee = sp.select_one("#fee_1 > table > tfoot > tr > th:nth-child(2)").text.strip()
                    tuitions.append({
                        "study_mode":"Part-time",
                        "duration":part_time,
                        "student_category":cat,
                        "fee":fee
                    })
            return tuitions
        except AttributeError:
            return []
        
    def _get_pg_about(self, soup):
        about = ""
        try:
            abouts = soup.find_all('p')
            for p in abouts[1:]:
                about += p.text
            return about
        except AttributeError:
            return ""

    def parse_pg_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'lxml')
        application_dates = self._get_pg_application_dates(soup)
        start_dates = self._get_pg_start_dates(soup)
        description = self._get_pg_description(soup)
        entry_req = self._get_pg_entry_requirements(soup)
        lang_req = self._get_pg_language_requirements(soup)
        fee = self._get_pg_fee(response.meta['code'], response.meta['full_time'], response.meta['part_time'])
        about = self._get_pg_about(soup)
        yield {
            'link': response.url,
            'title': response.meta['title'],
            'study_level': 'Graduate',
            'description': description,
            'university_title': 'University of Cambridge',
            'locations': [],
            'qualification': response.meta['qualification'],
            'modules': [],
            'tuitions': fee,
            'application_dates': application_dates,
            'start_dates': start_dates,
            'about': about,
            'entry_requirements': entry_req,
            'language_requirements': lang_req
        }
    def _get_jbs_description(self, soup):
        try:
            description = soup.select_one('.intro-description')
            return description.text.strip() if description else ""
        except AttributeError:
            return ""

    def _get_jbs_about(self, soup):
        about = ""
        try:
            ps = soup.find_all('p')
            for p in ps:
                about += p.text.strip()
            return about
        except AttributeError:
            return ""

    def _get_jbs_start_dates(self, soup):
        start_dates = []
        try:
            h6s = soup.find_all('h6')
            for h6 in h6s:
                if 'start' in h6.text.lower():
                    p = h6.find_next_sibling('p')
                    if p:
                        start_dates.append(p.text.strip())
            return start_dates
        except AttributeError:
            return []

    def _get_jbs_locations(self, soup):
        locations = []
        try:
            h6s = soup.find_all('h6')
            for h6 in h6s:
                if 'location' in h6.text.lower():
                    p = h6.find_next_sibling('p')
                    if p:
                        locations.append(p.text.strip())
            return locations
        except AttributeError:
            return []

    def _get_jbs_application_dates(self, soup):
        application_dates = []
        try:
            strongs = soup.select('strong')
            for strong in strongs:
                date_match = re.search(r'(\w+?\s?\d{1,2}[srnt]?[tdh]?\s\w+\s\d{4})', strong.text)
                if date_match:
                    application_dates.append(date_match.group(1))
            h2s = soup.find_all("h2", class_="wp-block-heading has-text-align-center")
            for h2 in h2s:
                application_dates.append(h2.text.strip())
            return application_dates
        except AttributeError:
            return []

    def _get_jbs_entry_requirements(self, soup):
        entry_req = ""
        try:
            divs = soup.find_all('div', class_='b01__accordion-content')
            for div in divs:
                entry_req += div.text.strip()
            return entry_req
        except AttributeError:
            return ""

    def _get_jbs_language_requirements(self, soup):
        lang_req = []
        try:
            accordions = soup.find_all('div', class_='b01__accordion')
            for accordion in accordions:
                btn = accordion.find('button')
                if 'language requirements' or 'english proficiency' in btn.text.lower():
                    lis = accordion.find_all('li')
                    for li in lis:
                        test = li.text.split(' ')[0]
                        score = li.text.split(test)[1]
                        lang_req.append({
                            'language': 'English',
                            'test': test,
                            'score': score.strip()
                        })
            return lang_req
        except AttributeError:
            return []

    def _get_jbs_fee(self, soup, full_time, part_time):
        fees = []
        try:
            fee = ''
            h2 = soup.find('h2', class_='wp-block-heading')
            ps = h2.find_next_siblings('p')
            txt = ""
            for p in ps:
                txt += p.text
            fee_match = re.search(r'([£]\d*,\d*)', txt)
            if fee_match:
                fee = fee_match.group(1)
            if full_time != '':
                fees.append({
                    "study_mode": "Full-time",
                    "duration": full_time,
                    "student_category": "",
                    "fee": fee
                })
            if part_time != '':
                fees.append({
                    "study_mode": "Part-time",
                    "duration": part_time,
                    "student_category": "",
                    "fee": fee
                })
            return fees
        except AttributeError:
            return []

    def _get_jbs_modules(self, soup, module_type):
        modules = []
        try:
            h5s = soup.find_all('h5')
            if not h5s:
                h5s = soup.find_all('h6')
            for h5 in h5s:
                modules.append({
                    'title': h5.text.strip(),
                    'type': module_type,
                    'link': ""
                })
            divs = soup.find_all('div', class_ = 'wp-block-column')
            for div in divs:
                ul = div.find('ul')
                if ul:
                    lis = ul.find_all('li')
                    for li in lis:
                        modules.append({
                        'title': li.text.strip(),
                        'type': "Optional",
                        'link': ""
                    })
            return modules
        except AttributeError:
            return []

    def parse_jbs_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'lxml')

        description = self._get_jbs_description(soup)
        about = self._get_jbs_about(soup)
        start_dates = self._get_jbs_start_dates(soup)
        locations = self._get_jbs_locations(soup)

        application_response = requests.get(response.url + 'apply/', headers={'User-agent': 'Mozilla/5.0'})
        application_soup = BeautifulSoup(application_response.content, 'lxml')
        application_dates = self._get_jbs_application_dates(application_soup)
        entry_req = self._get_jbs_entry_requirements(application_soup)
        lang_req = self._get_jbs_language_requirements(application_soup)

        fee_response = requests.get(response.url + 'fees-funding/', headers={'User-agent': 'Mozilla/5.0'})
        fee_soup = BeautifulSoup(fee_response.content, 'lxml')
        fee = self._get_jbs_fee(fee_soup, response.meta['full_time'], response.meta['part_time'])
        modules = []
        modules_response = requests.get(response.url + 'curriculum/courses/', headers={'User-agent': 'Mozilla/5.0'})
        module_soup = BeautifulSoup(modules_response.content, 'lxml')
        modules = self._get_jbs_modules(module_soup, 'Compulsory')
        if modules == []:
            modules_response = requests.get(response.url + 'curriculum/core-courses-and-electives/', headers={'User-agent': 'Mozilla/5.0'})
            module_soup = BeautifulSoup(modules_response.content, 'lxml')
            modules = self._get_jbs_modules(module_soup, 'Compulsory')
        if modules == []:
            core_modules_response = requests.get(response.url + 'curriculum/core-courses/', headers={'User-agent': 'Mozilla/5.0'})
            core_modules_soup = BeautifulSoup(core_modules_response.content, 'lxml')
            core_modules = self._get_jbs_modules(core_modules_soup, 'Compulsory')

            elective_modules_response = requests.get(response.url + 'curriculum/electives/', headers={'User-agent': 'Mozilla/5.0'})
            elective_modules_soup = BeautifulSoup(elective_modules_response.content, 'lxml')
            elective_modules = self._get_jbs_modules(elective_modules_soup, 'Optional')

            modules = core_modules + elective_modules

        yield {
            'link': response.url,
            'title': response.meta['title'],
            'study_level': 'Graduate',
            'description': description,
            'university_title': 'University of Cambridge',
            'locations': locations,
            'qualification': response.meta['qualification'],
            'modules': modules,
            'tuitions': fee,
            'application_dates': application_dates,
            'start_dates': start_dates,
            'about': about,
            'entry_requirements': entry_req,
            'language_requirements': lang_req
        }



def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(CambridgeSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
