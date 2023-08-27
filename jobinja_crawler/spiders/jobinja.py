import csv
import logging
import re

import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider


class JobinjaSpider(scrapy.Spider):
    name = 'jobinja'
    start_urls = ['https://jobinja.ir/companies']
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'jobinja_companies.csv',
    }
    crawled_count = 0
    current_page = 1  # To keep track of the current page number
    companies_df = pd.DataFrame([], columns=['company_name', 'company_url'])
    companies_full_info = pd.DataFrame([], columns=['Company Name', 'Company URL', 'Lower Employee Count', 'Upper Employee Count', 'Founded Year'])

    # Block: Reading the last crawled company to resume the crawling process
    try:
        with open('last_crawled.txt', 'r') as f:
            last_crawled = f.read().strip()
    except FileNotFoundError:
        last_crawled = None

    def parse(self, response):
        self.log(f'Visited {response.url}', logging.DEBUG)

        section_xpath = '//a[@class="c-companyOverview"]'  
        company_sections = response.xpath(section_xpath)

        if self.current_page > 10000:
            self.log('Reached the 10000th page. Stopping.', logging.INFO)
            return
        
        # Block: Iterating over the companies listed on the current page
        for company_section in company_sections:
            company_url = company_section.xpath('@href').get()
            company_name = company_section.xpath('.//h3/text()').get()
            company_name = company_name.strip() if company_name else ""
            
            if not company_name:
                self.log(f'Company name not found.', logging.ERROR)
                continue

            self.log(f'Extracted company name: {company_name}', logging.INFO)
            self.log(f'Extracted company URL: {company_url}', logging.INFO)

            self.companies_df = pd.concat([self.companies_df, pd.DataFrame([company_name, company_url])], ignore_index=True, axis=0)

            yield scrapy.Request(url=company_url,
                                callback=self.parse_company,
                                meta={'company_name': company_name, 'company_url': company_url})

        self.companies_df.to_csv("./jobinja_comp_data_pd.csv", encoding='utf-8')
        
        # Block: Navigating to the next page
        next_page = response.xpath('//a[@rel="next"]/@href').get()
        if next_page:
            self.current_page += 1
            yield scrapy.Request(url=next_page, callback=self.parse)
        else: 
            print("WHY NOT NEXT PAGE")
            
    def parse_company(self, response):
        company_name = response.meta['company_name']  # Get the passed company name
        company_url = response.meta['company_url']  # Get the passed company name

        # Log to indicate parse_company is being called
        self.log(f"Parsing company details for: {company_name}", logging.INFO)

        meta_section = response.css('.c-companyHeader__meta')

        # Fetching the business area
        business_area = meta_section.css('.c-companyHeader__metaLink::text').get()

        # Fetching the employee range
        emp_range = meta_section.css('.c-companyHeader__metaItem:nth-child(3)::text').get()
        lower_emp, upper_emp = re.findall(r'\d+', emp_range)
        lower_emp = 'N/A' if not lower_emp else lower_emp.translate(str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789'))
        upper_emp = 'N/A' if not upper_emp else upper_emp.translate(str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789'))

        # Fetching the founded year
        founded_year = meta_section.css('.c-companyHeader__metaItem:nth-child(1)::text').get()
        founded_year = re.findall(r'\d+', founded_year)
        founded_year = 'N/A' if (not founded_year or type(founded_year) != 'list') else founded_year[0].translate(str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789'))

        # Log extracted information
        self.log(f"Company Name: {company_name}, Employee Range: {lower_emp}-{upper_emp}, Business Area: {business_area}, Founded Year: {founded_year}", logging.INFO)

        self.companies_full_info = pd.concat([self.companies_full_info, pd.DataFrame([company_name, company_url, lower_emp, upper_emp, business_area, founded_year])], ignore_index=True, axis=0)

        if self.companies_full_info.shape[0] % 10 == 0:
            self.companies_df.to_csv("./jobinja_full_company_info.csv", encoding='utf-8')

        # Block: Saving the extracted information
        yield {
            'Company Name': company_name,
            'Lower Employee Count': lower_emp,
            'Upper Employee Count': upper_emp,
            'Business Area': business_area,
            'Founded Year': founded_year
        }

        self.crawled_count += 1

        # Block: Saving progress after every 10 companies
        if self.crawled_count % 10 == 0:
            with open('last_crawled.txt', 'w', encoding='utf-8') as f:
                f.write(company_name)
                raise CloseSpider('Batch of 10 companies crawled')
        
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    process.crawl(JobinjaSpider)
    process.start()
