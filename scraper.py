import requests
import os
import json
from datetime import datetime
import time
import re

class Scraper:
    def __init__(self, base_url="https://www.backmarket.es/reviews/v1/products/"):
        """
        Initializes the Scraper with the base URL.
        """
        self.base_url = base_url
        
    def fetch_product_reviews(self, product_id, order_by="-relevance_alt", locale="es-es", cursor=None):
        """
        Fetch product reviews with pagination support.
        
        Args:
            product_id (str): Product identifier
            order_by (str): Sort order for reviews, default is by relevance
            locale (str): Language for translations
            cursor (str): Pagination token for fetching next page of reviews
            
        Returns:
            dict: JSON response with reviews or error message
        """
        url = f"{self.base_url}{product_id}/reviews"
        
        # Build request parameters
        params = {
            "order_by": order_by,
            "translation_locale": locale
        }
        
        # Add cursor parameter for pagination if provided
        if cursor:
            params["cursor"] = cursor
            
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Failed to fetch reviews: {response.status_code}"}
    
    def process_reviews(self, reviews_data, product_id, product_name=None):
        """
        Process reviews data and extract required fields
        
        Args:
            reviews_data (dict): JSON response from fetch_product_reviews
            product_id (str): Product identifier
            product_name (str, optional): Name of the product. If not provided, will be extracted from reviews
            
        Returns:
            dict: Structured review data
        """
        # Extract product name from first review if not provided
        if not product_name and reviews_data.get("results"):
            first_review = reviews_data["results"][0]
            product_info = first_review.get("product", {})
            product_name = product_info.get("title", f"Product_{product_id}")
        
        # Create output structure
        processed_data = {
            "product_name": product_name,
            "product_id": product_id,
            "reviews": {}
        }
        
        
        # Process each review
        for idx, review in enumerate(reviews_data.get("results", [])):
            review_id = f"review_{idx + 1}"
            
            # Extract customer information
            customer = review.get("customer", {})
            
            # Extract required fields
            processed_review = {
                "comment": review.get("comment", ""),
                "createdAt": review.get("createdAt", ""),
                "first_name": customer.get("firstName", ""),
                "last_name": customer.get("lastName", ""),
                "details": []
            }
            
            # Extract complete seller information
            seller_info = review.get("seller", {})
            processed_review["seller"] = {
                "id": seller_info.get("id", ""),
                "companyName": seller_info.get("companyName", ""),
                "companySlug": seller_info.get("companySlug", "")
            }
            
            # Extract details (ratings)
            details = review.get("details", [])
            for i in range(min(5, len(details))):
                if i < len(details):
                    processed_review["details"].append({
                        "identifier": details[i].get("identifier", ""),
                        "rate": details[i].get("rate", 0)
                    })
        
            
            # Add review to processed data
            processed_data["reviews"][review_id] = processed_review
    
        return processed_data
    
    def save_processed_reviews(self, processed_data, output_file="processed_reviews.json"):
        """
        Save processed review data to JSON file
        
        Args:
            processed_data (dict): Processed review data
            output_file (str): Output file path
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(processed_data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"Error saving processed reviews: {str(e)}")
            return False
            
    def clean_filename(self, filename):
        """
        Clean filename to remove or replace characters that are invalid for file paths
        
        Args:
            filename (str): Original filename
            
        Returns:
            str: Cleaned filename safe for file system
        """
        # Remove or replace invalid characters for Windows file names
        invalid_chars = r'[<>:"/\\|?*]'
        cleaned = re.sub(invalid_chars, '_', filename)
        
        # Remove or replace quotation marks and other problematic characters
        cleaned = cleaned.replace('"', '')
        cleaned = cleaned.replace("'", '')
        cleaned = cleaned.replace('–', '-')  # Replace em dash
        cleaned = cleaned.replace('—', '-')  # Replace en dash
        
        # Remove multiple spaces and replace with single space
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Strip whitespace from start and end
        cleaned = cleaned.strip()
        
        # Limit length more aggressively to avoid path too long errors
        # Account for the fact that this will be used in: reviews/{name}/{name}_reviews.json
        # Windows has a 260 character limit for full paths
        max_length = 50  # Conservative limit to ensure full path stays under 260 chars
        if len(cleaned) > max_length:
            # Try to keep the most important parts (beginning and end)
            if ' - ' in cleaned:
                # Split by main separator and keep first part + a shortened version
                parts = cleaned.split(' - ')
                main_part = parts[0]
                if len(main_part) <= max_length:
                    cleaned = main_part
                else:
                    cleaned = main_part[:max_length-3] + "..."
            else:
                cleaned = cleaned[:max_length-3] + "..."
        
        # Remove trailing dots and spaces that Windows doesn't like
        cleaned = cleaned.rstrip('. ')
            
        return cleaned
            
    def get_reviews_path(self, product_name):
        """
        Generate a path for saving reviews based on product name
        and ensure the directory exists
        
        Args:
            product_name (str): Name of the product
            
        Returns:
            str: Path to save the reviews JSON file
        """
        # Clean the product name for use in file paths
        cleaned_product_name = self.clean_filename(product_name)
        
        # Create the directory path
        directory = os.path.join("reviews", cleaned_product_name)
        
        # Verify the full path length won't exceed Windows limits
        full_path = os.path.join(directory, f"{cleaned_product_name}_reviews.json")
        if len(os.path.abspath(full_path)) > 240:  # Leave some buffer
            # Further shorten the name
            cleaned_product_name = cleaned_product_name[:30] + "..."
            directory = os.path.join("reviews", cleaned_product_name)
            full_path = os.path.join(directory, f"{cleaned_product_name}_reviews.json")
        
        # Create directories if they don't exist
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"Directory created/verified: {directory}")
        except Exception as e:
            print(f"Error creating directory: {e}")
            # Fallback to a simple directory name
            cleaned_product_name = f"product_{int(time.time())}"
            directory = os.path.join("reviews", cleaned_product_name)
            os.makedirs(directory, exist_ok=True)
            full_path = os.path.join(directory, f"{cleaned_product_name}_reviews.json")
        
        print(f"Full path will be: {os.path.abspath(full_path)}")
        print(f"Path length: {len(os.path.abspath(full_path))} characters")
        
        return full_path

    def fetch_and_process_reviews(self, product_id, product_name=None, output_file=None, max_pages=None):
        """
        Fetch, process, and save reviews for a product with pagination and incremental saves
        
        Args:
            product_id (str): Product identifier
            product_name (str, optional): Name of the product. If not provided, will be extracted from reviews
            output_file (str, optional): Custom output file path or None to use default path
            max_pages (int, optional): Maximum number of pages to fetch
            
        Returns:
            dict: Processed review data
        """
        cursor = None
        page_count = 0
        total_reviews_processed = 0
        reviews_this_minute = 0
        minute_start_time = time.time()
        extracted_product_name = None
        
        # Create initial structure
        processed_data = {
            "product_name": product_name,
            "product_id": product_id,
            "reviews": {}
        }
        
        # Load existing data if file exists
        if output_file and os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    processed_data = json.load(f)
                    # Count existing reviews
                    total_reviews_processed = len(processed_data["reviews"])
                    print(f"Loaded {total_reviews_processed} existing reviews")
            except Exception as e:
                print(f"Error loading existing reviews: {str(e)}")
        
        # Continue fetching pages until no more results or max_pages reached
        while True:
            # Check rate limit - 250 reviews per minute
            current_time = time.time()
            elapsed = current_time - minute_start_time
            
            if elapsed >= 60:  # If a minute has passed, reset counter
                minute_start_time = current_time
                reviews_this_minute = 0
            elif reviews_this_minute >= 250:  # If we're approaching the rate limit
                sleep_time = 60 - elapsed
                print(f"Rate limit approaching. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                minute_start_time = time.time()
                reviews_this_minute = 0
                
            # Fetch reviews for current page
            print(f"Fetching page {page_count + 1} with cursor: {cursor}")
            reviews_data = self.fetch_product_reviews(product_id, cursor=cursor)
            
            # Check if we got results
            if "results" not in reviews_data or not reviews_data["results"]:
                print("No more reviews to fetch")
                break
            
            # Update rate limit counter
            reviews_this_minute += len(reviews_data.get("results", []))
            
            # Get the number of reviews in this batch
            num_reviews = len(reviews_data.get("results", []))
            print(f"Fetched {num_reviews} reviews")
            
            # Process reviews for this page
            page_processed_data = self.process_reviews(reviews_data, product_id, product_name)
            
            # Extract product name from first page if not provided
            if not extracted_product_name and page_processed_data.get("product_name"):
                extracted_product_name = page_processed_data["product_name"]
                processed_data["product_name"] = extracted_product_name
                
                # Set output file path if not provided
                if output_file is None:
                    output_file = self.get_reviews_path(extracted_product_name)
                    print(f"Product name extracted: {extracted_product_name}")
                    print(f"Reviews will be saved to: {output_file}")
            
            # Merge reviews into our main data structure
            for review_id, review_data in page_processed_data["reviews"].items():
                # Re-index the review based on total reviews processed
                new_review_id = f"review_{total_reviews_processed + int(review_id.split('_')[1])}"
                processed_data["reviews"][new_review_id] = review_data
                total_reviews_processed += 1
                
                # Save every 10 reviews
                if total_reviews_processed % 10 == 0 and output_file:
                    print(f"Saving after processing {total_reviews_processed} reviews")
                    self.save_processed_reviews(processed_data, output_file)
            
            # Check if there's a next page
            next_cursor = reviews_data.get("next")
            
            # If next_cursor is a full URL, extract just the cursor parameter
            if next_cursor and "cursor=" in next_cursor:
                import urllib.parse
                parsed_url = urllib.parse.urlparse(next_cursor)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                if "cursor" in query_params and query_params["cursor"]:
                    cursor = query_params["cursor"][0]
                else:
                    cursor = None
            else:
                cursor = next_cursor
                
            if not cursor:
                print("No cursor for next page, finished fetching")
                break
            
            # Increment page counter
            page_count += 1
            
            # Check if we've reached max_pages
            if max_pages and page_count >= max_pages:
                print(f"Reached maximum number of pages: {max_pages}")
                break
        
        # Final save
        if output_file and total_reviews_processed % 10 != 0:
            print(f"Final save with {total_reviews_processed} total reviews")
            self.save_processed_reviews(processed_data, output_file)
            
        return processed_data