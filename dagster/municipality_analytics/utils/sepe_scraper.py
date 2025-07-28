"""
SEPE (Servicio Público de Empleo Estatal) Web Scraper
Extracts unemployment data by municipalities from SEPE website
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import logging
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
import re

class SepeScraper:
    
    def __init__(self, base_url: str = "https://www.sepe.es", download_dir: str = "raw/sepe"):
        self.base_url = base_url
        self.download_dir = download_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Create download directory
        os.makedirs(download_dir, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def extract_month_number(self, month_identifier: str, year: str) -> int:
        """
        Extract numeric month from month identifier
        """
        # Spanish month names to numbers
        spanish_months = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        
        # Clean month identifier - remove year and extra characters
        month_clean = month_identifier.lower()
        
        # Remove year from the identifier if present
        month_clean = re.sub(rf'-?{year}', '', month_clean)
        month_clean = month_clean.strip('-').strip()
        
        # Try to find Spanish month name
        for spanish_month, number in spanish_months.items():
            if spanish_month in month_clean:
                return number
        
        # Try to extract number directly if already numeric
        number_match = re.search(r'(\d{1,2})', month_clean)
        if number_match:
            month_num = int(number_match.group(1))
            if 1 <= month_num <= 12:
                return month_num
        
        # Default fallback
        self.logger.warning(f"Could not extract month number from '{month_identifier}', defaulting to 1")
        return 1
    
    def get_available_years_months(self) -> Dict[str, List[str]]:
        """
        Extract available years and months from the main SEPE municipalities page
        """
        main_url = f"{self.base_url}/HomeSepe/es/que-es-el-sepe/estadisticas/datos-estadisticos/municipios.html"
        
        try:
            response = self.session.get(main_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all links that start with the municipios base URL
            year_month_links = {}
            base_municipios_url = "/HomeSepe/que-es-el-sepe/estadisticas/datos-estadisticos/municipios/"
            
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                
                # Check if link starts with the municipios base URL
                if href.startswith(base_municipios_url):
                    # Extract year from URL path (try different patterns)
                    year_match = re.search(r'/municipios/.*?(\d{4})', href)
                    
                    if year_match:
                        year = year_match.group(1)
                        
                        # Extract a meaningful identifier for the month/period
                        # This could be the filename or a part of the path
                        path_parts = href.split('/')
                        month_identifier = path_parts[-1] if path_parts else href
                        
                        # Remove .html extension if present
                        month_identifier = month_identifier.replace('.html', '')
                        
                        if year not in year_month_links:
                            year_month_links[year] = []
                        
                        # Avoid duplicates
                        existing_urls = [item['url'] for item in year_month_links[year]]
                        full_url = urljoin(self.base_url, href)
                        
                        if full_url not in existing_urls:
                            year_month_links[year].append({
                                'month_identifier': month_identifier,
                                'url': full_url,
                                'link_text': link.get_text(strip=True),
                                'original_href': href
                            })
            
            # Sort by year and log results
            for year in year_month_links:
                year_month_links[year].sort(key=lambda x: x['month_identifier'])
            
            total_links = sum(len(months) for months in year_month_links.values())
            self.logger.info(f"Found {total_links} links across {len(year_month_links)} years")
            
            # Log sample of found links for debugging
            for year, months in list(year_month_links.items())[:3]:  # Show first 3 years
                self.logger.info(f"Year {year}: {len(months)} links")
                for month in months[:2]:  # Show first 2 months per year
                    self.logger.info(f"  - {month['month_identifier']}: {month['url']}")
            
            return year_month_links
            
        except Exception as e:
            self.logger.error(f"Error getting available years/months: {e}")
            return {}
    
    def get_month_page_data(self, month_url: str) -> Optional[Dict]:
        """
        Extract data from a specific month page
        """
        try:
            response = self.session.get(month_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for "Libro completo" text and find XLS link near it
            libro_completo_link = None
            
            # Method 1: Look for "Libro completo" text and find nearby XLS links
            page_text = soup.get_text()
            if 'libro completo' in page_text.lower():
                # Find all XLS links on the page
                all_xls_links = []
                for link in soup.find_all('a', href=True):
                    href = link.get('href')
                    if href.endswith(('.xls', '.xlsx')):
                        all_xls_links.append(urljoin(self.base_url, href))
                
                # Look for the XLS link that appears after "Libro completo" in the HTML structure
                soup_str = str(soup)
                libro_completo_pos = soup_str.lower().find('libro completo')
                
                if libro_completo_pos != -1:
                    # Look for XLS links after the "Libro completo" text
                    remaining_html = soup_str[libro_completo_pos:]
                    xls_match = re.search(r'href="([^"]*\.xls[^"]*)"', remaining_html, re.IGNORECASE)
                    
                    if xls_match:
                        libro_completo_link = urljoin(self.base_url, xls_match.group(1))
                        self.logger.info(f"Found Libro completo XLS: {libro_completo_link}")
                
                # Fallback: if we found XLS links but couldn't match the pattern, take the first one
                if not libro_completo_link and all_xls_links:
                    libro_completo_link = all_xls_links[0]
                    self.logger.info(f"Using first XLS link as fallback: {libro_completo_link}")
            
            # Method 2: Look for table structure with td elements (as backup)
            if not libro_completo_link:
                # Look for td elements containing "Libro completo" and adjacent td with XLS link
                for td in soup.find_all('td'):
                    if 'libro completo' in td.get_text().lower():
                        # Look for sibling td elements with XLS links
                        parent_row = td.find_parent('tr')
                        if parent_row:
                            for sibling_td in parent_row.find_all('td'):
                                xls_link = sibling_td.find('a', href=re.compile(r'\.xls', re.IGNORECASE))
                                if xls_link:
                                    libro_completo_link = urljoin(self.base_url, xls_link.get('href'))
                                    break
                        if libro_completo_link:
                            break
            
            # Method 3: General XLS link search as final fallback
            if not libro_completo_link:
                excel_links = []
                for link in soup.find_all('a', href=True):
                    href = link.get('href')
                    if href.endswith(('.xls', '.xlsx')):
                        excel_links.append(urljoin(self.base_url, href))
                
                if excel_links:
                    libro_completo_link = excel_links[0]
                    self.logger.info(f"Using general XLS search fallback: {libro_completo_link}")
            
            return {
                'url': month_url,
                'libro_completo_url': libro_completo_link,
                'all_excel_links': [urljoin(self.base_url, link.get('href')) 
                                  for link in soup.find_all('a', href=True) 
                                  if link.get('href', '').endswith(('.xls', '.xlsx'))]
            }
            
        except Exception as e:
            self.logger.error(f"Error processing month page {month_url}: {e}")
            return None
    
    def download_excel_file(self, excel_url: str, filename: str = None) -> Optional[str]:
        """
        Download Excel file from SEPE
        """
        try:
            if not filename:
                # Extract filename from URL
                parsed_url = urlparse(excel_url)
                filename = os.path.basename(parsed_url.path)
                if not filename.endswith(('.xls', '.xlsx')):
                    filename += '.xlsx'
            
            file_path = os.path.join(self.download_dir, filename)
            
            # Skip if file already exists
            if os.path.exists(file_path):
                self.logger.info(f"File already exists: {filename}")
                return file_path
            
            self.logger.info(f"Downloading: {excel_url}")
            response = self.session.get(excel_url, stream=True)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            self.logger.info(f"Downloaded: {filename}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error downloading {excel_url}: {e}")
            return None
    
    def scrape_all_available_data(self, years: List[str] = None, max_files: int = None) -> List[str]:
        """
        Scrape all available SEPE unemployment data
        """
        year_month_data = self.get_available_years_months()
        downloaded_files = []
        
        # Filter by specified years if provided
        if years:
            year_month_data = {year: months for year, months in year_month_data.items() 
                             if year in years}
        
        file_count = 0
        
        for year, months in year_month_data.items():
            self.logger.info(f"Processing year: {year}")
            
            for month_info in months:
                if max_files and file_count >= max_files:
                    break
                
                month_url = month_info['url']
                month_identifier = month_info['month_identifier']
                
                self.logger.info(f"Processing: {year} - {month_identifier}")
                
                # Get month page data
                month_data = self.get_month_page_data(month_url)
                
                if month_data and month_data['libro_completo_url']:
                    # Download the complete book file with standardized naming
                    # Convert month name to number
                    month_number = self.extract_month_number(month_identifier, year)
                    filename = f"{year}_{month_number:02d}_employment.xls"
                    downloaded_file = self.download_excel_file(
                        month_data['libro_completo_url'], 
                        filename
                    )
                    
                    if downloaded_file:
                        downloaded_files.append(downloaded_file)
                        file_count += 1
                else:
                    self.logger.warning(f"No 'Libro completo' found for {year} - {month_identifier}")
                
                # Respectful delay between requests
                time.sleep(1)
                
                if max_files and file_count >= max_files:
                    break
        
        self.logger.info(f"Downloaded {len(downloaded_files)} files")
        return downloaded_files
    
    def get_latest_data(self) -> Optional[str]:
        """
        Get the most recent unemployment data file
        """
        year_month_data = self.get_available_years_months()
        
        # Get the latest year and month
        if not year_month_data:
            return None
        
        latest_year = max(year_month_data.keys())
        latest_months = year_month_data[latest_year]
        
        if not latest_months:
            return None
        
        # Assume the last month in the list is the most recent
        latest_month = latest_months[-1]
        
        month_data = self.get_month_page_data(latest_month['url'])
        
        if month_data and month_data['libro_completo_url']:
            month_number = self.extract_month_number(latest_month['month_identifier'], latest_year)
            filename = f"{latest_year}_{month_number:02d}_employment.xls"
            return self.download_excel_file(month_data['libro_completo_url'], filename)
        
        return None


def main():
    """
    Test the scraper
    """
    scraper = SepeScraper()
    
    # Test: Get available data
    available_data = scraper.get_available_years_months()
    print(f"Available years: {list(available_data.keys())}")
    
    # Test: Download latest data
    latest_file = scraper.get_latest_data()
    if latest_file:
        print(f"Latest file downloaded: {latest_file}")
    else:
        print("No latest file could be downloaded")


if __name__ == "__main__":
    main()