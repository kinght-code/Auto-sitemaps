# Install dependencies
pip install -r requirements.txt

# Generate sitemap for Daily Amardesh
python app.py https://example.com

# Or use the class directly
python app.py https://example.com

# With custom parameters
python app.py https://example.com --max-urls 10000 --output-dir my_sitemaps
