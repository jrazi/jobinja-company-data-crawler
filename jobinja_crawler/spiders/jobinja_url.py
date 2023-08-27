import csv
import logging
import re

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider


class JobinjaCompanyUrlSpider(scrapy.Spider):
    name = 'jobinja_url'
    start_urls = ['https://jobinja.ir/companies']
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'jobinja_companies.csv',
    }
    crawled_count = 0
    current_page = 1
    companies_df = pd.DataFrame([], columns=['company_name', 'company_url'])

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

            new_entry = pd.DataFrame([[company_name, company_url]], columns=['company_name', 'company_url'])
            
            self.companies_df = pd.concat([self.companies_df, new_entry], ignore_index=True, axis=0)

        self.companies_df.to_csv("./jobinja_company_url.csv", encoding='utf-8')
        
        # Block: Navigating to the next page
        next_page = response.xpath('//a[@rel="next"]/@href').get()
        if next_page:
            self.current_page += 1
            yield scrapy.Request(url=next_page, callback=self.parse)
        else:
            print(f"*** REACHED THE LAST PAGE {self.current_page} ****")
            

        self.crawled_count += 1
        
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    process.crawl(JobinjaCompanyUrlSpider)
    process.start()
