"""
@Author: Md Mahodi Atik Shuvo
@Date: 25-05-2024.
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


class LeedsSpider(scrapy.Spider):

    name = 'leeds'
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University of Leeds'
    study_level = 'Graduate'

    english_language_certificate_map = {}
    modules_link_map= {}

    start_urls = [
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=1&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=2&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=3&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=4&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=5&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=6&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=7&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=8&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=9&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=10&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=11&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=12&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=13&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=14&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=15&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=16&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=17&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=18&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=19&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=20&term=202425',
        'https://courses.leeds.ac.uk/course-search/masters-courses?query=&type=PGT&page=21&term=202425'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(LeedsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        # English language minimum requirements
        # IELTS (6.5 overall with no less than 6.0 in each component skill)
        # yield scrapy.Request(
        #     url='https://www.leeds.ac.uk/international-admissions/doc/entry-requirements',
        #     callback=self.parse_english_minimum_requirements,
        #     priority=3)

        # English language equivalent qualifications for TOEFL and PTE
        yield scrapy.Request(
            url='https://www.leeds.ac.uk/language-centre/doc/english-language-certificates',
            callback=self.parse_english_equivalent_qualifications,
            priority=2)
        
        #parse_module_links
        yield scrapy.Request(
            url="https://webprod3.leeds.ac.uk/catalogue/modulesearch.asp?L=TP&Y=202425&E=all&N=all&S=+&A=any",
            callback=self.parse_module_links,
            meta=dict(
                            playwright=True,
                            playwright_include_page=True,
                            errback=self.errback
                            ))


        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    # def parse_english_minimum_requirements(self, response: HtmlResponse):
    #     soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
    #     certificates = []

    #     master_degrees_requirements = soup.find(
    #         text=re.compile("Masters degrees"))\
    #         .find_next().select('li')
    #     for certificate in master_degrees_requirements:
    #         tests = certificate.select('strong')
    #         if tests:
    #             test = " or ".join([x.text.strip() for x in tests])
    #             if 'LanguageCert International ESOL' in test:
    #                 score = certificate.get_text(separator='|', strip=True)\
    #                     .split('|')[-3]\
    #                     .replace('\xa0', ' ')\
    #                     .split('. ')[0]
    #             else:
    #                 score = certificate.get_text(separator='|', strip=True)\
    #                     .split('|')[-1]\
    #                     .replace('\xa0', ' ')\
    #                     .split('. ')[0]
    #             certificates.append({
    #                 'test': test.replace('\xa0', ' '),
    #                 'score': score
    #             })
    #     certificates = certificates[1:]
    #     self.english_language_certificate_map['6.5 overall, 6.0 in all'] = certificates

    def parse_english_equivalent_qualifications(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        certificates = {}
        scores = []
        toefl = soup.find('h3', text=re.compile("TOEFL iBT"))\
            .find_next()\
            .find_next()
        pearson_test_of_english = soup.find(
            'h3',
            text=re.compile("Pearson Test of English"))\
            .find_next()
        languagecert= soup.find('h3', text=re.compile("LanguageCert Academic"))\
            .find_next()

        table_headers = toefl.select('th')[1:]
        for table_header in table_headers:
            content = table_header.text.split(' ')[-1].strip()
            if content not in certificates:
                certificates[content] = {}
                scores.append(content)

        for (name, table) in [('TOEFL', toefl),
                                ('PTE', pearson_test_of_english),
                                ('LanguageCert',languagecert)]:
            rows = table.select('tr')
            for row in rows[1:]:
                cols = row.select('td')
                skill = cols[0].text.split(' ')[0].lower().replace('\xa0', '')
                for i, col in enumerate(cols):
                    if name not in certificates[scores[i-1]]:
                        certificates[scores[i-1]][name] = {
                                skill: col.text.strip()}
                    else:
                        certificates[scores[i-1]][name][skill] = col.text\
                                                                    .strip()
        self.english_language_certificate_map = certificates

    async def parse_module_links(self, response: HtmlResponse):
        page = response.meta["playwright_page"]
        await page.close()
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        tables = soup.find_all('table', {'width': '100%'})
        for table in tables:
            for row in table.find_all('tr'):
                name= row.select('td')[3].text.strip().lower()
                link= row.select('td')[2].find('a')['href']
                link= f'https://webprod3.leeds.ac.uk/catalogue/{link}'
                self.modules_link_map[name]= link


    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        ok= soup.select('h2 a')
        for i in ok:
            title=i.text.strip()
            link= i.get('href')
            qualification=i.text.strip().split(" ")[-1]
            if(qualification=="(Eng)"):
                qualification=i.text.split(" ")[-2]
            if(qualification=="Certificate"):
                qualification="Postgraduate Certificate"
            if(qualification=="Diploma"):
                qualification="Postgraduate Diploma"
            durations= i.findNext('dl').text.replace("Duration","").strip()
            yield scrapy.Request(url=link,
                                 callback=self.parse_course,
                                 dont_filter=True,
                                 meta={
                                     'qualification': qualification,
                                     'title': title,
                                     'durations': durations,
                                     'link': link,

                                 })

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title = soup.find('h1').text
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, soup: Tag) -> Optional[str]:
        try:
            qualification = soup.find('h1').text.split(' ')[-1]
        except AttributeError:
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = ['Woodhouse, Leeds LS2 9JT, United Kingdom']
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description = soup.find(attrs={"name": "Description"})['content']
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about = soup.select_one('#section-overview').prettify()
        except AttributeError:
            about = None
        return about

    def _get_tuitions(self, soup: BeautifulSoup,durations) -> list:
        try:
            tuitions=[]
            durations="12 months full time"
            for duration in durations.split(','):
                if "full time" in duration:
                    study_mode="Full-time"
                elif "part time" in duration:
                    study_mode="Part-time"
                selector=soup.select('#main .uol-key-facts dt')
                for i in selector:
                    if(i.text.lower()=="uk fees"):
                        student_category="UK"
                        fee=i.findNext('dd').text
                        tuitions.append({"student_category":student_category, "study_mode":study_mode, "duration":duration, "fee":fee})
                    elif(i.text.lower()=="international fees"):
                        student_category="International"
                        fee=i.findNext('dd').text
                        tuitions.append({"student_category":student_category, "study_mode":study_mode, "duration":duration, "fee":fee})
        except AttributeError:
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
           start_dates = []
           selector=soup.select('#main .uol-key-facts dt')
           for i in selector:
                if(i.text.lower()=="start date"):
                    date=i.findNext('dd').text
                    start_dates.append(date)
                elif(i.text.lower()=="start dates"):
                    try:
                        for j in i.findNextSiblings('dd'):
                            start_dates.append(j.text)
                    except:
                        pass
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
           application_dates= []
           selector=soup.select_one('#section-applying')
           date_pattern= re.compile(r'\d{2} \w+ \d{4}')
           dates= date_pattern.findall(selector.text)
           for date in dates:
               application_dates.append(date)
        except AttributeError:
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements_section = soup.find(
                    'h3',
                    string='Entry requirements')
            content = []
            for tag in entry_requirements_section.next_siblings:
                if tag.name == 'h3' or tag.name == 'h4':
                    break
                content.append(str(tag))
            entry_requirements = "".join(content)
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    # def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
    #     try:
    #         certificates = dict()
    #         english_language_requirements = []
    #         english_requirements_li = None

    #         key_facts = soup.find(attrs={"id": "keyfacts"}).select('li')
    #         for li in key_facts:
    #             if li.find(text=re.compile("Language requirements:")):
    #                 english_requirements_li = li
    #                 break

    #         if english_requirements_li:
    #             ielts_score = english_requirements_li.select('span')[1]\
    #                 .text.strip().lower().split(' ')
    #             test = ielts_score[0]
    #             ielts_score = " ".join(ielts_score[1:])
    #             certificates[test] = ielts_score

    #             score = ielts_score.replace('in in ', 'in ')\
    #                 .replace('spoken', 'speaking')\
    #                 .split('in ')
    #             overall = score[0][0:3]
    #             all = ['reading', 'writing', 'speaking', 'listening']
    #             for i in range(1, len(score)):
    #                 part = score[i]
    #                 skill_score = score[i-1].strip()[-3:]
    #                 for skill in all[:]:
    #                     if skill in part:
    #                         for test_name, skills in self.english_language_certificate_map[skill_score].items():
    #                             if test_name in certificates:
    #                                 certificates[test_name] += ' and ' + skills[skill] \
    #                                         + ' in ' + skill + ' '
    #                             else:
    #                                 overall_score = self.english_language_certificate_map[overall][test_name]['overall']
    #                                 certificates[test_name] = overall_score \
    #                                     + ' overall, with no less than ' \
    #                                     + skills[skill] + ' in ' + skill
    #                         all.remove(skill)

    #             if len(score) == 1 and len(score[0].split(',')) == 1:
    #                 skill_score = overall
    #                 for test_name, skills in self.english_language_certificate_map[skill_score].items():
    #                     certificates[test_name] = skills['overall'] \
    #                             + ' overall'
    #             elif len(all) > 0:
    #                 if len(all) == 4:
    #                     skill_score = overall
    #                     for test_name, skills in self.english_language_certificate_map[skill_score].items():
    #                         certificates[test_name] = skills['overall'] \
    #                                 + ' overall, with no less than  '
    #                 if len(score) == 1:
    #                     skill_score = skill_score = score[0].strip()[-3:]
    #                 else:
    #                     skill_score = skill_score = score[-2].strip()[-3:]
    #                 for test_name, skills in self.english_language_certificate_map[skill_score].items():
    #                     for i, skill in enumerate(all):
    #                         if i != 0:
    #                             certificates[test_name] += ' and'
    #                         certificates[test_name] += ' ' + skills[skill] + ' in ' \
    #                             + skill

    #             if overall == '6.5' and len(all) == 4:
    #                 test = 'IELTS (International English Language Testing System) Academic or IELTS for UKVI (United Kingdom Visas and Immigration) (Academic) or IELTS Online'
    #                 english_language_requirements.append({
    #                     'test': test,
    #                     'score': ielts_score
    #                 })
    #                 english_language_requirements += self.english_language_certificate_map['6.5 overall, 6.0 in all']
    #             else:
    #                 for key, value in certificates.items():
    #                     if key == 'ielts':
    #                         test = 'IELTS (International English Language Testing System) Academic or IELTS for UKVI (United Kingdom Visas and Immigration) (Academic) or IELTS Online'
    #                     elif key == 'toefl':
    #                         test = 'TOEFL iBT (Test of English as a Foreign Language Internet-Based Test) or TOEFL iBT Home Edition'
    #                     elif key == 'pte':
    #                         test = 'PTE (Pearson Test of English) Academic or PTE Academic UKVI or PTE Academic Online'
    #                     english_language_requirements.append({
    #                         'test': test,
    #                         'score': value
    #                     })
    #     except (AttributeError, KeyError):
    #         english_language_requirements = []
    #     return english_language_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            language_requirements = []
            selector=soup.select_one("#section-applying div h4")
            ok=selector.find_next("p").text
            ielts_pattern= re.compile(r"IELTS (\d+\.\d+)")
            point= ielts_pattern.search(ok).group(1)
            language_requirements.append({"language":"English","test":"IELTS","score":point})
            certificates = self.english_language_certificate_map
            for cert in certificates[point].keys():
                # print(cert, certificates[score][cert])
                scores = []
                for score in certificates[point][cert].keys():
                    scores.append(f'{score}: {certificates[point][cert][score]}')
                # print(', '.join(scores))

                language_requirements.append({
                    "language": "English",
                    "test": cert,
                    "score": ', '.join(scores)
                })
        except AttributeError:
            try:
                language_requirements.append({"language":"English","test":"IELTS","score":"6.5"}) #as minimum requirements described in https://www.leeds.ac.uk/international-admissions/doc/entry-requirements
                certificates = self.english_language_certificate_map
                for cert in certificates['6.5'].keys():
                    # print(cert, certificates[score][cert])
                    scores = []
                    for score in certificates['6.5'][cert].keys():
                        scores.append(f'{score}: {certificates["6.5"][cert][score]}')
                    # print(', '.join(scores))

                    language_requirements.append({
                        "language": "English",
                        "test": cert,
                        "score": ', '.join(scores)
                    })
            except:
                language_requirements = []
        return language_requirements



    def _get_modules(self, soup: BeautifulSoup) -> list:
        try:
            try:
                modules=[]
                #for modules in table case
                selector=soup.select('#section-content  table tr')
                for i in selector:
                    if(i.select('th')):
                        continue
                    title= i.select('td')[0].text.strip()
                    type_text=i.find_previous('h4').text.lower().strip()
                    if(type_text.find("optional")!=-1):
                        type="Optional"
                    else:
                        type="Compulsory"
                    try:
                        link= self.modules_link_map[title.lower().strip()]
                    except:
                        link= ""
                    modules.append({"title":title, "type":type,"link":link})
            except:
                pass
            try:
                language=soup.select_one('h1.page-heading__title')
                if(language.text.lower().find("language for")!=-1):
                    title=language.text.strip()
                    type="Compulsory"
                    link=""
                    modules.append({"title":title, "type":type,"link":link})
            except:
                pass
            try:
                #for module in strong tag case
                selector=soup.select('#section-content div p strong')
                for i in selector:
                    title= i.text.strip()
                    if(title=="Optional modules" or title=="Compulsory modules"):
                        continue
                    type_text=i.find_previous('h3').text.lower().strip()
                    if(type_text.find("optional")!=-1):
                        type="Optional"
                    else:
                        type="Compulsory"
                    try:
                        finder= title.split("(")[0].strip().lower()
                        link= self.modules_link_map[finder]
                    except:
                        link= ""
                    modules.append({"title":title, "type":type,"link":link})
            except:
                pass
            try:
                #for module in li tag case
                selector=soup.select('#section-content div li')
                for i in selector:
                    title= i.text.split("-")[0].strip()
                    if(title=="Optional modules" or title=="Compulsory modules"):
                        continue
                    try:
                        type_text=i.find_previous('h3').text.lower().strip()
                    except:
                        type_text=i.find_previous('strong').text.lower().strip()
                    if(type_text.find("optional")!=-1):
                        type="Optional"
                    else:
                        type="Compulsory"
                    try:
                        finder= title.split("(")[0].strip().lower()
                        link= self.modules_link_map[finder]
                    except:
                        link= ""
                    modules.append({"title":title, "type":type,"link":link})
            except:
                pass
        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        title = response.meta['title']
        study_level = self.study_level
        qualification = response.meta['qualification']
        university = self.university
        locations = self._get_locations(soup)  # location not available
        description = self._get_description(soup)
        about = self._get_about(soup)
        durations= response.meta['durations']
        tuitions = self._get_tuitions(soup,durations)
        start_dates = self._get_start_dates(soup)
        

        # TODO application_dates available, but not structured
        application_dates = self._get_application_dates(soup)

        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)

        # TODO try to scrape modules from catalog to fetch links
        # https://webprod3.leeds.ac.uk/catalogue/modulesearch.asp?Y=202223&T=S&L=TP
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

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()

def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(LeedsSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
