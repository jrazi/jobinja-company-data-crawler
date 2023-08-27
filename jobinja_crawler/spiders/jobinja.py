import csv
import logging
import re

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider


class JobinjaSpider(scrapy.Spider):
    name = 'jobinja'
    crawled_count = 0
    companies_df = pd.read_csv('jobinja_company_url.csv')
    
    start_urls = companies_df['company_url'].to_list()
    def parse(self, response):
        
        company_name_element = response.css('.c-companyHeader__name')
        company_name_fa = company_name_element.xpath('text()').get()
        company_name_en = company_name_element.xpath('span/following-sibling::text()').get()
        company_name_en = 'N/A' if not company_name_en else company_name_en.strip()
        company_name_fa = 'N/A' if not company_name_fa else company_name_fa.strip()
    
        company_url = response.url
        
        # Log to indicate parse_company is being called
        self.log(f"Parsing company details for: {company_name_en}", logging.INFO)

        meta_section = response.css('.c-companyHeader__meta')

        # Fetching the business area
        business_area = meta_section.css('.c-companyHeader__metaLink::text').get().strip()

        # Fetching the employee range
        emp_range = meta_section.css('.c-companyHeader__metaItem:nth-child(3)::text').get().strip()
        emp_range_digits = re.findall(r'\d+', emp_range)
        
        lower_emp = None if (not emp_range_digits or len(emp_range_digits) < 1)  else emp_range_digits[0]
        upper_emp = None if (not emp_range_digits or len(emp_range_digits) <= 1)  else emp_range_digits[1]
        
        lower_emp = 'N/A' if not lower_emp else lower_emp.translate(str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')).strip()
        upper_emp = 'N/A' if not upper_emp else upper_emp.translate(str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')).strip()

        # Fetching the founded year
        founded_year = meta_section.css('.c-companyHeader__metaItem:nth-child(1)::text').get()
        founded_year = re.findall(r'\d+', founded_year)
        founded_year = 'N/A' if (not founded_year or len(founded_year) < 1) else founded_year[0].translate(str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789'))

        # Log extracted information
        self.log(f"Company Name: {company_name_en}, Employee Range: {lower_emp}-{upper_emp}, Business Area: {business_area}, Founded Year: {founded_year}", logging.INFO)

        # Block: Saving the extracted information
        yield {
            'Company Name (EN)': company_name_en,
            'Company Name (FA)': company_name_fa,
            'Company URL': company_url,
            'Lower Employee Count': lower_emp,
            'Upper Employee Count': upper_emp,
            'Business Area': business_area,
            'Founded Year': founded_year
        }

        self.crawled_count += 1
        
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    process.crawl(JobinjaSpider)
    process.start()
