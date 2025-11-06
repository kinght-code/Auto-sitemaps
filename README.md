python -m venv venv

source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Generate sitemap 
python app.py https://example.com

# With custom parameters
python app.py https://example.com --max-urls 10000 --output-dir my_sitemaps
