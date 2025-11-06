#!/usr/bin/env python3

import urllib.request
import urllib.error
import urllib.parse
from urllib.parse import urljoin, urlparse
import datetime
import time
import re
import os
import json
import ssl
from collections import defaultdict
import argparse

# Bypass SSL verification
ssl._create_default_https_context = ssl._create_unverified_context

class FixedSitemapGenerator:
    def __init__(self, base_url: str, max_urls_per_sitemap: int = 50000, max_crawl_pages: int = 1000):
        self.base_url = base_url.rstrip('/')
        self.max_urls_per_sitemap = max_urls_per_sitemap
        self.max_crawl_pages = max_crawl_pages
        self.all_urls = []

    def fetch_url(self, url: str, max_retries: int = 3):
        """Fetch URL with proper headers and error handling"""
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (compatible; FixedSitemapGenerator/2.0)',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                }

                req = urllib.request.Request(url, headers=headers)
                response = urllib.request.urlopen(req, timeout=15)
                content = response.read().decode('utf-8', errors='ignore')

                time.sleep(0.5)  # Respectful delay
                return content

            except Exception as e:
                print(f"   ⚠️ Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return None

    def discover_existing_sitemaps(self):
        """Discover existing sitemap files"""
        print("🔍 Discovering existing sitemaps...")

        sitemap_locations = [
            "/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml",
            "/wp-sitemap.xml", "/sitemap.php", "/sitemap.txt",
            "/sitemap_news.xml", "/sitemap_video.xml", "/sitemap_image.xml",
            "/sitemap-mobile.xml", "/sitemap-news.xml", "/sitemap-posts.xml"
        ]

        found_sitemaps = []

        # Check robots.txt first
        robots_url = f"{self.base_url}/robots.txt"
        content = self.fetch_url(robots_url)
        if content:
            for line in content.split('\n'):
                line = line.strip()
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    found_sitemaps.append(sitemap_url)
                    print(f"   📄 Found in robots.txt: {sitemap_url}")

        # Check common sitemap locations
        for location in sitemap_locations:
            sitemap_url = f"{self.base_url}{location}"
            content = self.fetch_url(sitemap_url)
            if content:
                content_lower = content.lower()
                if any(tag in content_lower for tag in ['<urlset', '<sitemapindex', 'sitemap']):
                    found_sitemaps.append(sitemap_url)
                    print(f"   📄 Found sitemap: {sitemap_url}")
                else:
                    print(f"   ❓ Found file but not a sitemap: {sitemap_url}")

        return found_sitemaps

    def extract_urls_from_sitemap(self, sitemap_url: str):
        """Extract URLs from a sitemap file with better error handling"""
        print(f"   📖 Reading: {sitemap_url}")
        urls = []

        content = self.fetch_url(sitemap_url)
        if not content:
            print(f"   ❌ Failed to fetch: {sitemap_url}")
            return urls

        try:
            # Remove XML namespaces for simpler parsing
            content_clean = re.sub(r'xmlns[^>]*', '', content)

            # Handle sitemap index
            if '<sitemapindex' in content_clean.lower():
                sitemap_locs = re.findall(r'<loc>(.*?)</loc>', content_clean)
                print(f"   📑 Found sitemap index with {len(sitemap_locs)} child sitemaps")

                for child_sitemap in sitemap_locs[:3]:  # Process first 3 to avoid too many requests
                    child_urls = self.extract_urls_from_sitemap(child_sitemap)
                    urls.extend(child_urls)
                    time.sleep(1)
            else:
                # Regular sitemap - extract URL entries
                url_blocks = re.findall(r'<url>(.*?)</url>', content_clean, re.DOTALL)

                for block in url_blocks:
                    loc_match = re.search(r'<loc>(.*?)</loc>', block)
                    lastmod_match = re.search(r'<lastmod>(.*?)</lastmod>', block)
                    changefreq_match = re.search(r'<changefreq>(.*?)</changefreq>', block)
                    priority_match = re.search(r'<priority>(.*?)</priority>', block)

                    if loc_match:
                        url_data = self.analyze_and_categorize_url(loc_match.group(1))
                        url_data.update({
                            'lastmod': lastmod_match.group(1) if lastmod_match else datetime.datetime.now().strftime('%Y-%m-%d'),
                            'changefreq': changefreq_match.group(1) if changefreq_match else 'weekly',
                            'priority': priority_match.group(1) if priority_match else '0.5',
                            'source': 'existing_sitemap'
                        })
                        urls.append(url_data)

        except Exception as e:
            print(f"   ❌ Error parsing sitemap {sitemap_url}: {e}")

        print(f"   ✅ Extracted {len(urls)} URLs from {sitemap_url}")
        return urls

    def crawl_website_from_homepage(self):
        """Crawl website starting from homepage to discover all accessible links"""
        print("🌐 Crawling website from homepage...")

        visited = set()
        to_visit = [self.base_url]
        discovered_urls = []
        page_count = 0

        while to_visit and page_count < self.max_crawl_pages:
            current_url = to_visit.pop(0)

            if current_url in visited:
                continue

            page_count += 1
            if page_count % 10 == 0:
                print(f"   📊 Progress: {page_count} pages crawled, {len(discovered_urls)} URLs found")

            content = self.fetch_url(current_url)

            if content:
                visited.add(current_url)

                # Categorize and add current URL
                url_data = self.analyze_and_categorize_url(current_url)
                url_data.update({
                    'lastmod': datetime.datetime.now().strftime('%Y-%m-%d'),
                    'source': 'crawler'
                })
                discovered_urls.append(url_data)

                # Extract links from page
                links = self.extract_links_from_html(content, current_url)

                for link in links:
                    if (self.is_valid_url(link) and
                        link not in visited and
                        link not in to_visit and
                        len(to_visit) < 500):  # Limit queue size
                        to_visit.append(link)

        print(f"   ✅ Crawling completed: {len(discovered_urls)} URLs found from {page_count} pages")
        return discovered_urls

    def extract_links_from_html(self, html_content: str, base_url: str):
        """Extract all links from HTML content"""
        links = []

        # Find all href attributes
        href_pattern = r'href=[\'"]([^\'"]*?)[\'"]'
        matches = re.findall(href_pattern, html_content, re.IGNORECASE)

        for match in matches:
            try:
                full_url = urljoin(base_url, match)
                if self.is_valid_url(full_url):
                    links.append(full_url)
            except Exception:
                continue  # Skip invalid URLs

        return links

    def is_valid_url(self, url: str):
        """Validate if URL should be included in sitemap"""
        if not url.startswith(self.base_url):
            return False

        # Exclude common file types
        excluded_extensions = [
            '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.ico',
            '.pdf', '.doc', '.docx', '.zip', '.rar',
            '.mp4', '.mp3', '.avi', '.mov',
            '.css', '.js', '.woff', '.ttf'
        ]

        if any(url.lower().endswith(ext) for ext in excluded_extensions):
            return False

        # Exclude common non-content URLs
        excluded_patterns = [
            '/cdn-cgi/', '/wp-admin/', '/wp-json/', '/api/', '/ajax/',
            '/logout', '/login', '/signin', '/signup', '/register',
            '/admin', '/dashboard', '/backend',
            '/cart', '/checkout', '/account',
            '?replytocom=', '?share=', '?feed=', '?s=',
            '#', 'tel:', 'mailto:', 'javascript:'
        ]

        if any(pattern in url.lower() for pattern in excluded_patterns):
            return False

        return True

    def analyze_and_categorize_url(self, url: str):
        """Analyze URL and categorize it with proper directory structure"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            path_parts = [part for part in path.split('/') if part]

            # Ensure all required fields are present
            url_data = {
                'loc': url,
                'directory_path': path,
                'path_parts': path_parts,
                'depth': len(path_parts),
                'changefreq': 'weekly',
                'priority': '0.5',
                'category': 'other'
            }

            # Homepage - Highest priority
            if url == self.base_url or url == f"{self.base_url}/":
                url_data.update({
                    'category': 'homepage',
                    'priority': '1.0',
                    'changefreq': 'daily'
                })
                return url_data

            # Contact pages
            if any(term in path for term in ['/contact', '/connect', '/get-in-touch']):
                url_data.update({
                    'category': 'contact',
                    'priority': '0.8'
                })
                return url_data

            # About pages
            if any(term in path for term in ['/about', '/about-us', '/company']):
                url_data.update({
                    'category': 'about',
                    'priority': '0.8'
                })
                return url_data

            # Article/News pages
            if any(pattern in url.lower() for pattern in ['/article/', '/news/', '/blog/', '/post/', '/story/']):
                url_data.update({
                    'category': 'articles',
                    'priority': '0.8',
                    'changefreq': 'daily'
                })
                return url_data

            # Category pages (first level directories)
            if len(path_parts) == 1 and path_parts[0] not in ['about', 'contact', 'privacy', 'terms']:
                url_data.update({
                    'category': 'main_categories',
                    'priority': '0.9',
                    'changefreq': 'daily'
                })
                return url_data

            # Sub-category pages (second level directories)
            if len(path_parts) == 2:
                url_data.update({
                    'category': 'subcategories',
                    'priority': '0.7',
                    'changefreq': 'weekly'
                })
                return url_data

            # Deep content (third level and beyond)
            if len(path_parts) >= 3:
                url_data.update({
                    'category': 'deep_content',
                    'priority': '0.6',
                    'changefreq': 'monthly'
                })
                return url_data

            # Legal pages
            if any(term in path for term in ['/privacy', '/terms', '/policy', '/disclaimer']):
                url_data.update({
                    'category': 'legal',
                    'priority': '0.3',
                    'changefreq': 'yearly'
                })
                return url_data

            return url_data

        except Exception as e:
            # Fallback for any parsing errors
            print(f"   ⚠️ Error analyzing URL {url}: {e}")
            return {
                'loc': url,
                'directory_path': '/',
                'path_parts': [],
                'depth': 0,
                'changefreq': 'weekly',
                'priority': '0.5',
                'category': 'other',
                'source': 'error_fallback'
            }

    def organize_urls_by_directory(self, urls):
        """Organize URLs by their directory structure with error handling"""
        print("📁 Organizing URLs by directory structure...")

        directory_map = defaultdict(list)
        category_map = defaultdict(list)

        for url_data in urls:
            try:
                # Ensure directory_path exists
                if 'directory_path' not in url_data:
                    url_data['directory_path'] = urlparse(url_data['loc']).path

                path = url_data['directory_path']
                path_parts = [part for part in path.split('/') if part]

                # Group by top-level directory
                if path_parts:
                    top_dir = path_parts[0]
                    directory_map[top_dir].append(url_data)
                else:
                    # Homepage
                    directory_map['homepage'].append(url_data)

                # Also categorize by content type
                category = url_data.get('category', 'other')
                category_map[category].append(url_data)

            except Exception as e:
                print(f"   ⚠️ Error organizing URL {url_data.get('loc', 'unknown')}: {e}")
                # Add to fallback category
                category_map['other'].append(url_data)

        print(f"   📂 Organized into {len(directory_map)} directories and {len(category_map)} categories")
        return directory_map, category_map

    def discover_all_urls(self):
        """Comprehensive URL discovery using multiple methods"""
        print("🚀 Starting comprehensive URL discovery...")

        all_urls = []

        # Method 1: Extract from existing sitemaps
        existing_sitemaps = self.discover_existing_sitemaps()
        for sitemap_url in existing_sitemaps:
            urls = self.extract_urls_from_sitemap(sitemap_url)
            all_urls.extend(urls)
            time.sleep(1)

        # Method 2: Always crawl website from homepage to get fresh links
        print("🌐 Starting comprehensive website crawl from homepage...")
        crawled_urls = self.crawl_website_from_homepage()
        all_urls.extend(crawled_urls)

        # Method 3: Generate essential URLs
        essential_urls = self.generate_essential_urls()
        all_urls.extend(essential_urls)

        # Remove duplicates
        unique_urls = self.deduplicate_urls(all_urls)

        # Organize by directory structure
        directory_map, category_map = self.organize_urls_by_directory(unique_urls)

        total_urls = len(unique_urls)
        print(f"✅ URL discovery completed! Found {total_urls} unique URLs across {len(directory_map)} directories")

        return unique_urls, directory_map, category_map

    def generate_essential_urls(self):
        """Generate essential URLs that should be in every sitemap"""
        essential_paths = [
            "/", "/home", "/index",
            "/about", "/about-us",
            "/contact", "/contact-us",
            "/privacy", "/privacy-policy",
            "/terms", "/terms-of-service",
            "/news", "/blog", "/articles"
        ]

        essential_urls = []
        for path in essential_paths:
            url = f"{self.base_url}{path}"
            url_data = self.analyze_and_categorize_url(url)
            url_data['lastmod'] = datetime.datetime.now().strftime('%Y-%m-%d')
            url_data['source'] = 'generated'
            essential_urls.append(url_data)

        return essential_urls

    def deduplicate_urls(self, urls):
        """Remove duplicate URLs"""
        unique_urls = []
        seen = set()

        for url_data in urls:
            url = url_data.get('loc', '')
            if url and url not in seen:
                unique_urls.append(url_data)
                seen.add(url)

        return unique_urls

    def generate_sitemap_index(self, sitemap_files):
        """Generate main sitemap index file with all discovered links"""
        filename = "sitemap_index.xml"
        today = datetime.datetime.now().strftime('%Y-%m-%d')

        print("📑 Generating sitemap index with all discovered links...")

        with open(filename, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

            for sitemap_file in sitemap_files:
                # Use absolute URLs in sitemap index
                sitemap_url = f"{self.base_url}/{sitemap_file}"

                f.write('  <sitemap>\n')
                f.write(f'    <loc>{self.escape_xml(sitemap_url)}</loc>\n')
                f.write(f'    <lastmod>{today}</lastmod>\n')
                f.write('  </sitemap>\n')

            f.write('</sitemapindex>\n')

        print(f"   ✅ Generated: {filename} with {len(sitemap_files)} sitemap references")
        return filename

    def generate_directory_sitemap(self, directory_name: str, urls):
        """Generate sitemap for a specific directory"""
        if not urls:
            return None

        # Create filename based on directory name
        if directory_name == 'homepage':
            filename = "sitemap.xml"
        else:
            # Clean directory name for filename
            clean_name = re.sub(r'[^a-zA-Z0-9_-]', '', directory_name.lower())
            filename = f"sitemap-{clean_name}.xml"

        print(f"   📄 Generating directory sitemap: {filename} ({len(urls)} URLs)")

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

                for url_data in urls:
                    f.write('  <url>\n')
                    f.write(f'    <loc>{self.escape_xml(url_data["loc"])}</loc>\n')
                    f.write(f'    <lastmod>{url_data.get("lastmod", datetime.datetime.now().strftime("%Y-%m-%d"))}</lastmod>\n')
                    f.write(f'    <changefreq>{url_data.get("changefreq", "weekly")}</changefreq>\n')
                    f.write(f'    <priority>{url_data.get("priority", "0.5")}</priority>\n')
                    f.write('  </url>\n')

                f.write('</urlset>\n')

            return filename

        except Exception as e:
            print(f"   ❌ Error generating sitemap {filename}: {e}")
            return None

    def generate_hierarchical_sitemaps(self, directory_map, category_map):
        """Generate hierarchical sitemaps based on directory structure"""
        print("🏗️ Generating hierarchical sitemaps by directory structure...")

        sitemap_files = []

        # Strategy 1: Create sitemaps by directory (primary organization)
        print("   📂 Creating directory-based sitemaps...")
        for directory_name, urls in directory_map.items():
            if urls:
                # If directory has too many URLs, split it
                if len(urls) > self.max_urls_per_sitemap:
                    num_chunks = (len(urls) // self.max_urls_per_sitemap) + 1
                    print(f"   📦 Splitting {directory_name} into {num_chunks} sitemaps")

                    for i in range(num_chunks):
                        start_idx = i * self.max_urls_per_sitemap
                        end_idx = start_idx + self.max_urls_per_sitemap
                        chunk_urls = urls[start_idx:end_idx]

                        if chunk_urls:
                            chunk_name = f"{directory_name}-part{i+1}"
                            sitemap_file = self.generate_directory_sitemap(chunk_name, chunk_urls)
                            if sitemap_file:
                                sitemap_files.append(sitemap_file)
                else:
                    sitemap_file = self.generate_directory_sitemap(directory_name, urls)
                    if sitemap_file:
                        sitemap_files.append(sitemap_file)

        # Strategy 2: If no directory sitemaps created, use categories
        if not sitemap_files:
            print("   🎯 Creating category-based sitemaps as fallback...")
            for category_name, urls in category_map.items():
                if urls:
                    sitemap_file = self.generate_directory_sitemap(category_name, urls)
                    if sitemap_file:
                        sitemap_files.append(sitemap_file)

        # Strategy 3: If still no sitemaps, create a single sitemap
        if not sitemap_files and directory_map:
            print("   🔄 Creating single combined sitemap...")
            all_urls = []
            for urls in directory_map.values():
                all_urls.extend(urls)
            sitemap_file = self.generate_directory_sitemap("all", all_urls)
            if sitemap_file:
                sitemap_files.append(sitemap_file)

        return sitemap_files

    def escape_xml(self, text: str):
        """Escape XML special characters"""
        if not text:
            return ""

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

    def generate_comprehensive_report(self, all_urls, directory_map, category_map, sitemap_files):
        """Generate detailed analytics report"""
        print("📊 Generating comprehensive analytics report...")

        report = {
            'generation_info': {
                'generated_at': datetime.datetime.now().isoformat(),
                'base_url': self.base_url,
                'total_sitemap_files': len(sitemap_files),
                'sitemap_index_url': f"{self.base_url}/sitemap_index.xml"
            },
            'url_statistics': {
                'total_urls': len(all_urls),
                'directories_count': len(directory_map),
                'categories_count': len(category_map)
            },
            'directory_breakdown': {},
            'category_breakdown': {},
            'sitemap_files': sitemap_files
        }

        # Directory breakdown
        for directory, urls in directory_map.items():
            report['directory_breakdown'][directory] = {
                'url_count': len(urls),
                'sample_urls': [url['loc'] for url in urls[:2]]
            }

        # Category breakdown
        for category, urls in category_map.items():
            report['category_breakdown'][category] = {
                'url_count': len(urls)
            }

        # Save detailed report
        with open('sitemap-analysis-report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        # Print executive summary
        self.print_executive_summary(report)

        return report

    def print_executive_summary(self, report):
        """Print professional executive summary"""
        print("\n" + "="*70)
        print("🎉 SITEMAP GENERATION COMPLETED!")
        print("="*70)

        stats = report['url_statistics']

        print(f"📊 EXECUTIVE SUMMARY:")
        print(f"   🌐 Website: {self.base_url}")
        print(f"   📈 Total URLs: {stats['total_urls']}")
        print(f"   📁 Directories: {stats['directories_count']}")
        print(f"   🏷️  Categories: {stats['categories_count']}")
        print(f"   📄 Sitemap Files: {report['generation_info']['total_sitemap_files']}")

        if report['directory_breakdown']:
            print(f"\n📂 DIRECTORY BREAKDOWN:")
            for directory, data in sorted(report['directory_breakdown'].items(),
                                        key=lambda x: x[1]['url_count'], reverse=True)[:8]:
                print(f"   • {directory:<20} {data['url_count']:>6} URLs")

        print(f"\n🚀 NEXT STEPS:")
        print(f"   1. Submit to Google Search Console: {report['generation_info']['sitemap_index_url']}")
        print(f"   2. Check sitemap-analysis-report.json for detailed analytics")

        print("="*70)

    def generate_complete_sitemap_structure(self, output_dir: str = "fixed_sitemaps"):
        """Generate complete fixed sitemap structure"""
        print("🚀 Starting Sitemap Generator")
        print(f"🌐 Target Website: {self.base_url}")
        print("="*70)

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        original_dir = os.getcwd()
        os.chdir(output_dir)

        try:
            # Discover and organize all URLs
            all_urls, directory_map, category_map = self.discover_all_urls()

            if not all_urls:
                print("❌ No URLs found to generate sitemap")
                # Create at least a basic sitemap with homepage
                basic_urls = self.generate_essential_urls()
                if basic_urls:
                    sitemap_file = self.generate_directory_sitemap("basic", basic_urls)
                    if sitemap_file:
                        self.generate_sitemap_index([sitemap_file])
                        print("✅ Created basic sitemap with essential URLs")
                return

            # Generate hierarchical sitemaps based on directory structure
            sitemap_files = self.generate_hierarchical_sitemaps(directory_map, category_map)

            # Generate professional sitemap index
            if sitemap_files:
                index_file = self.generate_sitemap_index(sitemap_files)

                # Generate comprehensive report
                self.generate_comprehensive_report(all_urls, directory_map, category_map, sitemap_files)

                print(f"\n✅ sitemap structure generated successfully!")
                print(f"📁 Output directory: {output_dir}")

            else:
                print("❌ No sitemap files were generated")

        except Exception as e:
            print(f"❌ Error during sitemap generation: {e}")
            import traceback
            traceback.print_exc()
        finally:
            os.chdir(original_dir)

def main():
    """Command line interface for the fixed sitemap generator"""
    parser = argparse.ArgumentParser(
        description='Sitemap Generator - Robust sitemap generation with better error handling'
    )

    parser.add_argument('url', help='Base URL of the website to generate sitemap for')
    parser.add_argument('--max-urls', type=int, default=50000,
                       help='Maximum URLs per sitemap file (default: 50000)')
    parser.add_argument('--max-crawl', type=int, default=1000,
                       help='Maximum pages to crawl (default: 1000)')
    parser.add_argument('--output-dir', default='sitemaps',
                       help='Output directory (default: sitemaps)')

    args = parser.parse_args()

    # Validate and normalize URL
    if not args.url.startswith(('http://', 'https://')):
        args.url = 'https://' + args.url

    # Create and run generator
    generator = FixedSitemapGenerator(
        base_url=args.url,
        max_urls_per_sitemap=args.max_urls,
        max_crawl_pages=args.max_crawl
    )

    generator.generate_complete_sitemap_structure(args.output_dir)

if __name__ == "__main__":
    main()
