from scraper import Scraper

if __name__ == "__main__":
    scraper = Scraper()
    product_id = "7c5494ef-0af5-4b28-bf2a-524b1d284464"
    
    # Fetch and process reviews - product name will be extracted automatically
    scraper.fetch_and_process_reviews(product_id)

