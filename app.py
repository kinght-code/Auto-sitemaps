import requests
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
import datetime
import time
import re
import os
import json
from typing import List, Dict

class AdvancedSitemapGenerator:
    def __init__(self, base_url: str, max_urls_per_sitemap: int = 50000):
        self.base_url = base_url.rstrip('/')
        self.max_urls_per_sitemap = max_urls_per_sitemap
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; SitemapGenerator/1.0)'
        })
        self.all_urls = []
        
    def fetch_url(self, url: str, max_retries: int = 3) -> requests.Response:
        """Fetch URL with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=10, allow_redirects=True)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return None

    def discover_urls(self, use_crawler: bool = True, max_pages: int = 100) -> List[Dict]:
        """Discover URLs from the website"""
        print(f"🔍 Discovering URLs from {self.base_url}")
        
        discovered_urls = []
        
        # Method 1: Check for existing sitemaps
        sitemap_urls = self.find_existing_sitemaps()
        if sitemap_urls:
            discovered_urls.extend(self.extract_from_existing_sitemaps(sitemap_urls))
        
        # Method 2: Crawl the website
        if use_crawler and len(discovered_urls) < 50:
            print("🌐 Crawling website to discover more URLs...")
            crawled_urls = self.crawl_website(max_pages)
            discovered_urls.extend(crawled_urls)
        
        # Method 3: Generate common URLs based on website structure
        common_urls = self.generate_common_urls()
        discovered_urls.extend(common_urls)
        
        # Remove duplicates and clean URLs
        unique_urls = self.deduplicate_urls(discovered_urls)
        
        print(f"✅ Found {len(unique_urls)} unique URLs")
        return unique_urls

    def find_existing_sitemaps(self) -> List[str]:
        """Find existing sitemap files"""
        sitemap_locations = [
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemap-index.xml",
            "/wp-sitemap.xml",
            "/sitemap.php",
            "/robots.txt"
        ]
        
        found_sitemaps = []
        
        # Check robots.txt first
        robots_url = f"{self.base_url}/robots.txt"
        response = self.fetch_url(robots_url)
        if response and response.status_code == 200:
            for line in response.text.split('\n'):
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    found_sitemaps.append(sitemap_url)
                    print(f"📄 Found sitemap in robots.txt: {sitemap_url}")
        
        # Check common sitemap locations
        for location in sitemap_locations:
            sitemap_url = f"{self.base_url}{location}"
            response = self.fetch_url(sitemap_url)
            if response and response.status_code == 200:
                content = response.text.lower()
                if any(tag in content for tag in ['<urlset', '<sitemapindex']):
                    found_sitemaps.append(sitemap_url)
                    print(f"📄 Found sitemap: {sitemap_url}")
        
        return found_sitemaps

    def extract_from_existing_sitemaps(self, sitemap_urls: List[str]) -> List[Dict]:
        """Extract URLs from existing sitemaps"""
        all_urls = []
        
        for sitemap_url in sitemap_urls:
            print(f"📖 Reading: {sitemap_url}")
            response = self.fetch_url(sitemap_url)
            if response:
                urls = self.parse_sitemap_content(response.text, sitemap_url)
                all_urls.extend(urls)
                print(f"   Extracted {len(urls)} URLs")
                time.sleep(1)  # Be respectful
        
        return all_urls

    def parse_sitemap_content(self, content: str, source_url: str) -> List[Dict]:
        """Parse sitemap content"""
        urls = []
        
        # Remove XML namespaces for simpler parsing
        content_clean = re.sub(r'xmlns[^>]*', '', content)
        
        # Check if it's a sitemap index
        if '<sitemapindex' in content_clean.lower():
            sitemap_locs = re.findall(r'<loc>(.*?)</loc>', content_clean)
            for sitemap_loc in sitemap_locs:
                child_response = self.fetch_url(sitemap_loc)
                if child_response:
                    child_urls = self.parse_sitemap_content(child_response.text, sitemap_loc)
                    urls.extend(child_urls)
        else:
            # Regular sitemap - extract URL entries
            url_blocks = re.findall(r'<url>(.*?)</url>', content_clean, re.DOTALL)
            
            for block in url_blocks:
                loc_match = re.search(r'<loc>(.*?)</loc>', block)
                lastmod_match = re.search(r'<lastmod>(.*?)</lastmod>', block)
                changefreq_match = re.search(r'<changefreq>(.*?)</changefreq>', block)
                priority_match = re.search(r'<priority>(.*?)</priority>', block)
                
                if loc_match:
                    url_data = {
                        'loc': loc_match.group(1),
                        'lastmod': lastmod_match.group(1) if lastmod_match else datetime.datetime.now().strftime('%Y-%m-%d'),
                        'changefreq': changefreq_match.group(1) if changefreq_match else 'weekly',
                        'priority': priority_match.group(1) if priority_match else '0.5'
                    }
                    urls.append(url_data)
        
        return urls

    def crawl_website(self, max_pages: int = 100) -> List[Dict]:
        """Crawl website to discover URLs"""
        visited = set()
        to_visit = [self.base_url]
        discovered_urls = []
        
        while to_visit and len(visited) < max_pages:
            current_url = to_visit.pop(0)
            
            if current_url in visited:
                continue
                
            print(f"🕸️  Crawling: {current_url}")
            response = self.fetch_url(current_url)
            
            if response and response.status_code == 200:
                visited.add(current_url)
                
                # Add current URL
                discovered_urls.append({
                    'loc': current_url,
                    'lastmod': datetime.datetime.now().strftime('%Y-%m-%d'),
                    'changefreq': 'daily',
                    'priority': '0.8' if current_url == self.base_url else '0.6'
                })
                
                # Extract links from page
                links = re.findall(r'href=[\'"]([^\'"]*?)[\'"]', response.text)
                
                for link in links:
                    full_url = urljoin(current_url, link)
                    
                    # Filter relevant URLs
                    if (self.base_url in full_url and
                        full_url not in visited and
                        full_url not in to_visit and
                        not any(ext in full_url.lower() for ext in ['.jpg', '.png', '.pdf', '.css', '.js', '.ico'])):
                        to_visit.append(full_url)
            
            time.sleep(1)  # Be respectful
        
        return discovered_urls

    def generate_common_urls(self) -> List[Dict]:
        """Generate common URLs based on website structure"""
        common_paths = [
            "/", "/about", "/contact", "/privacy", "/terms",
            "/news", "/articles", "/blog", "/categories",
            "/national", "/international", "/sports", "/entertainment",
            "/business", "/technology", "/health", "/education"
        ]
        
        common_urls = []
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        
        for path in common_paths:
            url = f"{self.base_url}{path}"
            common_urls.append({
                'loc': url,
                'lastmod': today,
                'changefreq': 'daily' if path == '/' else 'weekly',
                'priority': '1.0' if path == '/' else '0.7'
            })
        
        return common_urls

    def deduplicate_urls(self, urls: List[Dict]) -> List[Dict]:
        """Remove duplicate URLs"""
        unique_urls = []
        seen = set()
        
        for url_data in urls:
            if url_data['loc'] not in seen:
                unique_urls.append(url_data)
                seen.add(url_data['loc'])
        
        return unique_urls

    def categorize_urls(self, urls: List[Dict]) -> Dict[str, List[Dict]]:
        """Categorize URLs by type/path"""
        categories = {
            'homepage': [],
            'articles': [],
            'categories': [],
            'pages': [],
            'other': []
        }
        
        for url_data in urls:
            url = url_data['loc']
            
            if url == self.base_url or url == f"{self.base_url}/":
                categories['homepage'].append(url_data)
            elif any(pattern in url.lower() for pattern in ['/article/', '/news/', '/blog/', '/post/']):
                categories['articles'].append(url_data)
            elif any(pattern in url.lower() for pattern in ['/category/', '/section/', '/topic/']):
                categories['categories'].append(url_data)
            elif any(pattern in url for pattern in ['/about', '/contact', '/privacy', '/terms']):
                categories['pages'].append(url_data)
            else:
                categories['other'].append(url_data)
        
        return categories

    def generate_individual_sitemap(self, urls: List[Dict], sitemap_number: int) -> str:
        """Generate an individual sitemap file"""
        filename = f"sitemap_{sitemap_number}.xml"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
            
            for url_data in urls:
                f.write('  <url>\n')
                f.write(f'    <loc>{self.escape_xml(url_data["loc"])}</loc>\n')
                f.write(f'    <lastmod>{url_data["lastmod"]}</lastmod>\n')
                f.write(f'    <changefreq>{url_data["changefreq"]}</changefreq>\n')
                f.write(f'    <priority>{url_data["priority"]}</priority>\n')
                f.write('  </url>\n')
            
            f.write('</urlset>\n')
        
        print(f"📄 Generated: {filename} ({len(urls)} URLs)")
        return filename

    def generate_sitemap_index(self, sitemap_files: List[str]) -> str:
        """Generate sitemap index file"""
        filename = "sitemap_index.xml"
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
            
            for sitemap_file in sitemap_files:
                sitemap_url = f"{self.base_url}/{sitemap_file}" if not sitemap_file.startswith('http') else sitemap_file
                
                f.write('  <sitemap>\n')
                f.write(f'    <loc>{self.escape_xml(sitemap_url)}</loc>\n')
                f.write(f'    <lastmod>{today}</lastmod>\n')
                f.write('  </sitemap>\n')
            
            f.write('</sitemapindex>\n')
        
        print(f"📑 Generated: {filename}")
        return filename

    def escape_xml(self, text: str) -> str:
        """Escape XML special characters"""
        escapes = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&apos;'
        }
        
        for char, escape in escapes.items():
            text = text.replace(char, escape)
        
        return text

    def generate_complete_sitemap(self, output_dir: str = "sitemaps"):
        """Generate complete sitemap structure with index and individual sitemaps"""
        print("🚀 Starting sitemap generation...")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        os.chdir(output_dir)
        
        # Discover URLs
        all_urls = self.discover_urls(use_crawler=True, max_pages=50)
        
        if not all_urls:
            print("❌ No URLs found to generate sitemap")
            return
        
        # Split URLs into chunks for individual sitemaps
        sitemap_files = []
        total_urls = len(all_urls)
        
        if total_urls <= self.max_urls_per_sitemap:
            # Single sitemap
            filename = self.generate_individual_sitemap(all_urls, 1)
            sitemap_files.append(filename)
        else:
            # Multiple sitemaps
            num_sitemaps = (total_urls // self.max_urls_per_sitemap) + 1
            
            for i in range(num_sitemaps):
                start_idx = i * self.max_urls_per_sitemap
                end_idx = start_idx + self.max_urls_per_sitemap
                chunk_urls = all_urls[start_idx:end_idx]
                
                if chunk_urls:
                    filename = self.generate_individual_sitemap(chunk_urls, i + 1)
                    sitemap_files.append(filename)
        
        # Generate sitemap index
        index_file = self.generate_sitemap_index(sitemap_files)
        
        # Generate report
        self.generate_report(all_urls, sitemap_files)
        
        print(f"\n🎉 Sitemap generation completed!")
        print(f"📊 Total URLs: {total_urls}")
        print(f"📄 Sitemap files: {len(sitemap_files)}")
        print(f"📑 Index file: {index_file}")

    def generate_report(self, urls: List[Dict], sitemap_files: List[str]):
        """Generate a detailed report"""
        report = {
            'generated_at': datetime.datetime.now().isoformat(),
            'base_url': self.base_url,
            'total_urls': len(urls),
            'sitemap_files': sitemap_files,
            'urls_by_priority': {},
            'urls_by_changefreq': {},
            'categories': self.categorize_urls(urls)
        }
        
        # Count by priority
        for url_data in urls:
            priority = url_data['priority']
            report['urls_by_priority'][priority] = report['urls_by_priority'].get(priority, 0) + 1
        
        # Count by change frequency
        for url_data in urls:
            changefreq = url_data['changefreq']
            report['urls_by_changefreq'][changefreq] = report['urls_by_changefreq'].get(changefreq, 0) + 1
        
        with open('sitemap_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print("📊 Generated: sitemap_report.json")

def main():
    """Main function to run the sitemap generator"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate sitemap index and individual sitemaps')
    parser.add_argument('url', help='Base URL of the website')
    parser.add_argument('--max-urls', type=int, default=50000, help='Max URLs per sitemap (default: 50000)')
    parser.add_argument('--output-dir', default='sitemaps', help='Output directory (default: sitemaps)')
    
    args = parser.parse_args()
    
    # Create generator and generate sitemaps
    generator = AdvancedSitemapGenerator(args.url, args.max_urls)
    generator.generate_complete_sitemap(args.output_dir)

if __name__ == "__main__":
    main()
