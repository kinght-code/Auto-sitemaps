# Install dependencies
pip install -r requirements.txt

# Generate sitemap for Daily Amardesh
python generate_sitemap.py https://www.dailyamardesh.com

# Or use the class directly
python sitemap_generator.py https://www.dailyamardesh.com

# With custom parameters
python sitemap_generator.py https://www.dailyamardesh.com --max-urls 10000 --output-dir my_sitemaps
