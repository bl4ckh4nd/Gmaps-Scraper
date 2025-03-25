import json
import os
from playwright.sync_api import sync_playwright
from dataclasses import dataclass, asdict, field
import pandas as pd
import argparse
import time
import logging
import datetime
import re

l1=[]
l2=[]

Name = ""
Address = ""
Website = ""
Phone_Number = ""
Reviews_Count = 0
Reviews_Average = 0
Store_Shopping = ""
In_Store_Pickup = ""
Store_Delivery = ""
Place_Type = ""
Opens_At = ""
Introduction = ""

names_list=[]
address_list=[]
website_list=[]
phones_list=[]
reviews_c_list=[]
reviews_a_list=[]
store_s_list=[]
in_store_list=[]
store_del_list=[]
place_t_list=[]
open_list=[]
intro_list=[]

def extract_data(xpath, data_list, page):
    if page.locator(xpath).count() > 0:
        data = page.locator(xpath).inner_text()
    else:
        data = ""
    data_list.append(data)

def generate_grid(bounds, grid_size=2):
    """
    Generate a grid of coordinates within the specified bounds.
    
    Args:
        bounds: tuple of (min_lat, min_lng, max_lat, max_lng)
        grid_size: number of cells in each dimension (grid_size x grid_size total cells)
    
    Returns:
        List of (center_lat, center_lng, zoom) tuples for each grid cell
    """
    min_lat, min_lng, max_lat, max_lng = bounds
    lat_step = (max_lat - min_lat) / grid_size
    lng_step = (max_lng - min_lng) / grid_size
    
    grid_points = []
    
    for i in range(grid_size):
        for j in range(grid_size):
            center_lat = min_lat + (i + 0.5) * lat_step
            center_lng = min_lng + (j + 0.5) * lng_step
            # Use zoom level 16 as it's the sweet spot
            grid_points.append((center_lat, center_lng, 12))
    
    return grid_points

def save_progress(progress_data, filename='scraper_progress.json'):
    """Save progress data to a JSON file for job continuation"""
    with open(filename, 'w') as f:
        json.dump(progress_data, f)

def load_progress(filename='scraper_progress.json'):
    """Load progress data from a JSON file"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {
        "completed_cells": [],
        "seen_urls": [],
        "results_count": 0,
        "search_term": "",
        "bounds": [],
        "grid_size": 0,
        "total_target": 0
    }

def append_to_csv(new_data, filename='result.csv'):
    """Append new data to the CSV file with duplicate checking"""
    # Check if file exists
    if not os.path.exists(filename):
        # Create new file with header
        df = pd.DataFrame([new_data], columns=new_data.keys())
        df.to_csv(filename, index=False)
        return True
    else:
        # Read existing data to check for duplicates
        existing_df = pd.read_csv(filename)
        
        # Use name and address as a unique identifier
        if len(existing_df) > 0:
            # Check if this exact record already exists
            duplicate = existing_df[(existing_df['Names'] == new_data['Names']) & 
                                   (existing_df['Address'] == new_data['Address'])]
            
            if len(duplicate) > 0:
                # Skip duplicate
                print(f"Skipping duplicate: {new_data['Names']}")
                return False
        
        # If not a duplicate, append to the file
        df = pd.DataFrame([new_data])
        df.to_csv(filename, mode='a', header=False, index=False)
        return True

def extract_place_id(url):
    """Extract the unique place ID from a Google Maps URL"""
    try:
        # Look for the !19s pattern which is followed by the place ID
        if '!19s' in url:
            # Extract everything after !19s
            place_id = url.split('!19s')[1].split('!')[0]
            return place_id
        
        # Alternative method - look for the data= pattern
        elif 'data=' in url:
            parts = url.split('/')
            # Find the part with business ID
            for part in parts:
                if ':0x' in part:
                    return part
            
        # If neither method works, use the full URL (less efficient)
        return url
    except:
        return url

def parse_star_rating(star_text):
    """Extract numeric rating from star text like '5 Sterne'"""
    if not star_text:
        return 0
    
    # Extract the number from text like "5 Sterne" or "1 Stern"
    match = re.search(r'(\d+)', star_text)
    if match:
        return int(match.group(1))
    return 0

def extract_reviews(page, business_name, business_address, max_reviews=10):
    """Extract reviews for the current business listing"""
    logger = logging.getLogger()
    reviews = []
    
    try:
        # First check if we need to click on reviews tab - using improved selectors based on the tab structure
        # Try multiple selectors to improve robustness across different Google Maps versions and languages
        reviews_tab_selectors = [
            'xpath=//button[@role="tab" and contains(@aria-label, "Rezensionen")]',
            'xpath=//button[@role="tab" and contains(@aria-label, "Reviews")]',
            'xpath=//button[@role="tab"]//div[contains(text(), "Rezensionen")]/..',
            'xpath=//button[@role="tab"]//div[contains(text(), "Reviews")]/..',
            'xpath=//button[@role="tab" and @data-tab-index="1"]',  # Often the reviews tab is the second tab
            'xpath=//button[@jsaction="pane.rating.moreReviews"]'   # Original selector as fallback
        ]
        
        # Try each selector until we find a matching element
        reviews_tab_clicked = False
        for selector in reviews_tab_selectors:
            if page.locator(selector).count() > 0:
                logger.info(f"Found reviews tab with selector: {selector}")
                try:
                    page.locator(selector).click()
                    page.wait_for_timeout(2000)  # Wait for reviews to load
                    reviews_tab_clicked = True
                    break
                except Exception as e:
                    logger.warning(f"Error clicking reviews tab with selector {selector}: {e}")
        
        if not reviews_tab_clicked:
            logger.info("Could not find or click reviews tab, trying to continue with any available reviews")
        
        # Wait for review containers to appear
        review_containers_selector = 'xpath=//div[@class="jftiEf fontBodyMedium "]'
        try:
            page.wait_for_selector(review_containers_selector, timeout=5000)
        except:
            logger.info("No reviews found for this location")
            return reviews
        
        # Get initial review count
        review_containers = page.locator(review_containers_selector).all()
        logger.info(f"Found {len(review_containers)} initial reviews")
        
        # If we have reviews, try to load more by scrolling
        if len(review_containers) > 0:
            scroll_attempts = 0
            previous_count = 0
            
            # Find the scrollable container - it's usually a div with role="feed"
            feed_selectors = [
                'div[role="feed"]',  # CSS selector
                '.m6QErb',           # CSS selector for reviews container
                '[class*="m6QErb"]'   # CSS selector with wildcard for class name
            ]
            
            # Scroll the feed container to load more reviews
            while len(review_containers) < max_reviews and scroll_attempts < 5:
                # Try different scrolling methods
                try:
                    # Method 1: Use querySelector and JavaScript
                    for selector in feed_selectors:
                        try:
                            page.evaluate(f"""
                                const feed = document.querySelector('{selector}');
                                if (feed) {{
                                    feed.scrollTop = feed.scrollHeight;
                                    return true;
                                }}
                                return false;
                            """)
                            break
                        except Exception:
                            continue
                except Exception as e:
                    logger.warning(f"Error scrolling review feed with JS: {e}")
                    
                    # Method 2: Fallback to mouse wheel
                    try:
                        # Find a review element and scroll from there
                        if page.locator(review_containers_selector).count() > 0:
                            page.locator(review_containers_selector).first.scroll_into_view_if_needed()
                            page.mouse.wheel(0, 2000)
                        else:
                            page.mouse.wheel(0, 2000)
                    except Exception as e:
                        logger.warning(f"Fallback scrolling also failed: {e}")
                
                page.wait_for_timeout(2000)  # Wait for more reviews to load
                
                # Check if we got more reviews
                review_containers = page.locator(review_containers_selector).all()
                logger.info(f"After scroll: {len(review_containers)} reviews")
                
                if len(review_containers) <= previous_count:
                    scroll_attempts += 1
                else:
                    scroll_attempts = 0
                
                previous_count = len(review_containers)
        
        # Process all found reviews
        for i, container in enumerate(review_containers):
            if i >= max_reviews:
                break
                
            try:
                # Extract reviewer name - fixed selectors
                name_selectors = [
                    'xpath=.//div[contains(@class, "d4r55")]',
                    'css=div.d4r55',
                    '.d4r55'  # CSS shorthand
                ]
                
                reviewer_name = ""
                for selector in name_selectors:
                    try:
                        if container.locator(selector).count() > 0:
                            reviewer_name = container.locator(selector).inner_text()
                            break
                    except Exception:
                        continue
                
                # Extract review text - fixed selectors
                text_selectors = [
                    'xpath=.//div[@class="MyEned"]//span[@class="wiI7pd"]',
                    'css=div.MyEned span.wiI7pd',
                    '.wiI7pd'  # CSS shorthand
                ]
                
                review_text = ""
                for selector in text_selectors:
                    try:
                        if container.locator(selector).count() > 0:
                            review_text = container.locator(selector).inner_text()
                            break
                    except Exception:
                        continue
                
                # Extract star rating - fixed selectors
                stars_selectors = [
                    'xpath=.//span[@class="kvMYJc"]',
                    'css=span.kvMYJc',
                    '.kvMYJc'  # CSS shorthand
                ]
                
                stars = 0
                for selector in stars_selectors:
                    try:
                        if container.locator(selector).count() > 0:
                            stars_text = container.locator(selector).get_attribute('aria-label')
                            stars = parse_star_rating(stars_text)
                            break
                    except Exception:
                        continue
                
                # Extract review date - fixed selectors
                date_selectors = [
                    'xpath=.//span[@class="rsqaWe"]',
                    'css=span.rsqaWe',
                    '.rsqaWe'  # CSS shorthand
                ]
                
                date = ""
                for selector in date_selectors:
                    try:
                        if container.locator(selector).count() > 0:
                            date = container.locator(selector).inner_text()
                            break
                    except Exception:
                        continue
                
                # Extract owner response - fixed selectors
                response_selectors = [
                    'xpath=.//div[@class="CDe7pd"]//div[@class="wiI7pd"]',
                    'css=div.CDe7pd div.wiI7pd',
                    'div.CDe7pd .wiI7pd'  # CSS shorthand
                ]
                
                owner_response = ""
                for selector in response_selectors:
                    try:
                        if container.locator(selector).count() > 0:
                            owner_response = container.locator(selector).inner_text()
                            break
                    except Exception:
                        continue
                
                # Create review object
                review = {
                    'business_name': business_name,
                    'business_address': business_address,
                    'reviewer_name': reviewer_name,
                    'review_text': review_text,
                    'rating': stars,
                    'review_date': date,
                    'owner_response': owner_response,
                    'language': detect_language(review_text)
                }
                
                reviews.append(review)
                logger.debug(f"Extracted review {i+1}: {reviewer_name} - {stars} stars")
                
            except Exception as e:
                logger.error(f"Error extracting review {i+1}: {e}")
                continue
        
        logger.info(f"Successfully extracted {len(reviews)} reviews")
        return reviews
        
    except Exception as e:
        logger.error(f"Error in extract_reviews: {e}")
        return reviews

def detect_language(text):
    """Simple language detection based on common words"""
    if not text:
        return "unknown"
        
    # Simple language detection based on common words
    german_words = ['und', 'der', 'die', 'das', 'ist', 'sehr', 'gut', 'für', 'mit', 'von', 'nicht']
    english_words = ['and', 'the', 'is', 'very', 'good', 'for', 'with', 'not', 'this', 'that']
    
    text_lower = text.lower()
    words = re.findall(r'\b\w+\b', text_lower)
    
    german_count = sum(1 for word in words if word in german_words)
    english_count = sum(1 for word in words if word in english_words)
    
    if german_count > english_count:
        return "de"
    elif english_count > 0:
        return "en"
    else:
        return "unknown"

def save_reviews_to_csv(reviews, filename='reviews.csv'):
    """Save reviews to a CSV file"""
    if not reviews:
        return
    
    # Check if file exists to determine if header is needed
    file_exists = os.path.isfile(filename)
    
    # Create DataFrame from reviews
    df = pd.DataFrame(reviews)
    
    # Write to CSV, append if file exists
    df.to_csv(filename, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
    
    print(f"Saved {len(reviews)} reviews to {filename}")

# Set up logging configuration
def setup_logging():
    log_filename = f"scraper_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()

def main():
    # Set up logging
    logger = setup_logging()
    logger.info(f"Starting Google Maps scraper with search term: '{search_for}', target: {total} results")
    logger.info(f"Bounds: {bounds}, Grid size: {grid_size}x{grid_size}")

    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path='C:\Program Files\Google\Chrome\Application\chrome.exe', headless=False)
        page = browser.new_page()
        
        # Load progress if continuing a job
        progress = load_progress()
        
        # Initialize or load progress data
        if progress["search_term"] == search_for and progress["bounds"] == list(bounds) and progress["grid_size"] == grid_size:
            # Continuing previous job
            completed_cells = progress["completed_cells"]
            seen_urls = set(progress["seen_urls"])
            results_count = progress["results_count"]
            print(f"Continuing job: {results_count}/{total} results already collected")
        else:
            # New job
            completed_cells = []
            seen_urls = set()
            results_count = 0
            progress = {
                "completed_cells": completed_cells,
                "seen_urls": list(seen_urls),
                "results_count": results_count,
                "search_term": search_for,
                "bounds": list(bounds),
                "grid_size": grid_size,
                "total_target": total
            }
            save_progress(progress)
            
            # Create a new CSV file if starting a new job
            if os.path.exists('result.csv'):
                os.rename('result.csv', f'result_{time.strftime("%Y%m%d%H%M%S")}.csv')
        
        # After loading progress, synchronize with actual CSV records:

        try:
            if os.path.exists('result.csv'):
                df = pd.read_csv('result.csv')
                actual_count = len(df)
                if actual_count != results_count:
                    print(f"WARNING: Progress count ({results_count}) differs from actual CSV record count ({actual_count})")
                    results_count = actual_count
                    progress["results_count"] = results_count
                    save_progress(progress)
        except Exception as e:
            print(f"Error checking CSV record count: {e}")
        
        # Define the search area bounds
        search_bounds = bounds
        
        # Generate grid points
        grid_points = generate_grid(search_bounds, grid_size=grid_size)
        
        print(f"Split search into {len(grid_points)} grid areas")
        
        # For each grid point, perform a search
        for i, (lat, lng, zoom) in enumerate(grid_points):
            # Skip already completed cells
            cell_id = f"{i+1}/{len(grid_points)}"
            if cell_id in completed_cells:
                logger.info(f"Skipping already processed grid cell {cell_id}")
                continue
                
            logger.info(f"Searching grid cell {cell_id}...")
            
            try:
                # Go to Google Maps with specific coordinates and zoom level
                logger.info(f"Navigating to Google Maps at coordinates: {lat}, {lng}, zoom: {zoom}")
                page.goto(f"https://www.google.com/maps/@{lat},{lng},{zoom}z", timeout=60000)
                page.wait_for_timeout(1000)
                
                # Perform the search
                logger.info(f"Searching for: '{search_for}'")
                page.locator('//input[@id="searchboxinput"]').fill(search_for)
                page.keyboard.press("Enter")
                
                try:
                    logger.info("Waiting for search results...")
                    page.wait_for_selector('//a[contains(@href, "https://www.google.com/maps/place")]', timeout=10000)
                except Exception as e:
                    logger.warning(f"No results found in this grid cell: {e}")
                    completed_cells.append(cell_id)
                    progress["completed_cells"] = completed_cells
                    save_progress(progress)
                    continue
                
                # Your existing scroll logic
                results_selector = '[role="feed"]'
                previously_counted = 0
                max_attempts = 5  # Reduced from 5
                static_count_attempts = 0
                scroll_interval = 1500  # Reduced from 3000ms
                
                print("Starting to scroll for results...")
                
                while static_count_attempts < max_attempts:
                    current_count = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                    print(f"Currently Found: {current_count}")
                    
                    # Stop earlier if we find enough results
                    if current_count >= min(120, total - results_count):  # Increased from 100
                        print(f"Found sufficient results ({current_count}) in this grid cell")
                        break
                        
                    if current_count == previously_counted:
                        static_count_attempts += 1
                        print(f"No new results found. Attempt {static_count_attempts}/{max_attempts}")
                    else:
                        static_count_attempts = 0
                        previously_counted = current_count
                    
                    # More aggressive scrolling
                    if page.locator(results_selector).count() > 0:
                        try:
                            page.evaluate("""(selector) => {
                                const element = document.querySelector(selector);
                                if (element) {
                                    element.scrollTop = element.scrollHeight;
                                }
                            }""", results_selector)
                        except:
                            page.mouse.wheel(0, 20000)  # Increased scroll distance
                    else:
                        page.mouse.wheel(0, 20000)  # Increased scroll distance
                    
                    page.wait_for_timeout(scroll_interval)
                    
                    # Break early if we've scrolled enough
                    if current_count > 40 and static_count_attempts >= 2:
                        print("Breaking early - sufficient results found")
                        break
                
                # Improved listing collection with detailed logging
                try:
                    logger.info("Collecting listings...")
                    
                    # Get all potential listing elements
                    all_listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()
                    logger.info(f"Found {len(all_listings)} total listing elements")
                    
                    # Filter to only visible and accessible listings
                    grid_listings = []
                    invalid_listings = 0
                    invisible_listings = 0
                    
                    for idx, listing in enumerate(all_listings):
                        try:
                            if idx < 10 or idx % 10 == 0:  # Log first 10 and every 10th after that
                                logger.debug(f"Checking listing {idx+1}/{len(all_listings)}...")
                            
                            # Check if visible
                            is_visible = listing.is_visible()
                            if not is_visible:
                                invisible_listings += 1
                                continue
                            
                            # Try to get the href (with shorter timeout)
                            try:
                                href = listing.get_attribute('href', timeout=3000)
                                if href:
                                    grid_listings.append(listing)
                                else:
                                    invalid_listings += 1
                            except Exception:
                                invalid_listings += 1
                        except Exception as e:
                            logger.error(f"Error checking listing {idx}: {e}")
                    
                    logger.info(f"Results: {len(all_listings)} total, {invisible_listings} invisible, " +
                                f"{invalid_listings} invalid, {len(grid_listings)} usable")
                except Exception as e:
                    logger.error(f"Error collecting listings: {e}")
                    grid_listings = []
                
                # Process listings with better error handling and logging
                processed_count = 0
                skipped_count = 0
                error_count = 0

                # Replace the inner listing processing loop with this improved approach:

                try:
                    # First collect all listing URLs without clicking on them
                    logger.info("Collecting all listing URLs...")
                    all_listing_urls = []
                    all_listing_ids = []
                    
                    # Wait for listings to be available
                    page.wait_for_selector('//a[contains(@href, "https://www.google.com/maps/place")]', timeout=5000)
                    
                    # Get all URLs first
                    all_listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()
                    logger.info(f"Found {len(all_listings)} total listing elements")
                    
                    # Extract URLs and IDs first (without clicking)
                    for idx, listing in enumerate(all_listings):
                        try:
                            url = listing.get_attribute('href', timeout=3000)
                            place_id = extract_place_id(url)
                            
                            # Only add if not already seen
                            if place_id not in seen_urls:
                                all_listing_urls.append(url)
                                all_listing_ids.append(place_id)
                                logger.info(f"Added URL #{len(all_listing_urls)}: {url[:50]}... (ID: {place_id[:15]}...)")
                            else:
                                logger.info(f"Skipping already seen ID: {place_id[:15]}...")
                        except Exception as e:
                            logger.error(f"Error extracting URL {idx}: {e}")
                    
                    logger.info(f"Collected {len(all_listing_urls)} unique URLs to process")

                    # Define maximum listings to process per grid cell
                    max_listings_per_cell = 120  # Adjust this value based on your needs
                    
                    # Now process each URL directly
                    for idx, (url, place_id) in enumerate(zip(all_listing_urls, all_listing_ids)):
                        if results_count >= total:
                            logger.info(f"Reached target of {total} results, stopping")
                            break
                            
                        if idx >= max_listings_per_cell:
                            logger.info(f"Reached max of {max_listings_per_cell} listings for this cell, moving to next cell")
                            break
                        
                        try:
                            # Mark as seen before processing
                            seen_urls.add(place_id)
                            progress["seen_urls"] = list(seen_urls)
                            save_progress(progress)
                            
                            # Navigate directly to the URL
                            logger.info(f"Navigating to listing {idx+1}/{len(all_listing_urls)}: {url[:50]}...")
                            page.goto(url, timeout=30000)
                            
                            # Wait for details to load
                            logger.info("Waiting for listing details...")
                            page.wait_for_selector('//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]', timeout=10000)
                            logger.info("Details loaded successfully")
                            
                            # Process the listing data - your existing extraction code here
                            name_xpath = '//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]'
                            address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
                            website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
                            phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
                            reviews_count_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span//span//span[@aria-label]'
                            reviews_average_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span[@aria-hidden and contains(text(), ",")]'
                            info1='//div[@class="LTs0Rc"][1]'
                            info2='//div[@class="LTs0Rc"][2]'
                            info3='//div[@class="LTs0Rc"][3]'
                            opens_at_xpath='//button[contains(@data-item-id, "oh")]//div[contains(@class, "fontBodyMedium")]'
                            opens_at_xpath2='//div[@class="MkV9"]//span[@class="ZDu9vd"]//span[2]'
                            place_type_xpath='//div[@class="LBgpqf"]//button[@class="DkEaL "]'
                            intro_xpath='//div[@class="WeS02d fontBodyMedium"]//div[@class="PYvSYb "]'
                            
                            # Reset temporary data for this listing
                            name = ""
                            address = ""
                            website = ""
                            phone_number = ""
                            review_count = ""
                            review_average = ""
                            store_shopping = "No"
                            in_store_pickup = "No"
                            store_delivery = "No"
                            place_type = ""
                            opens_at = ""
                            introduction = "None Found"
                            
                            # Extract data with error handling
                            if page.locator(name_xpath).count() > 0:
                                name = page.locator(name_xpath).inner_text()
                                
                            if page.locator(address_xpath).count() > 0:
                                address = page.locator(address_xpath).inner_text()
                                
                            if page.locator(website_xpath).count() > 0:
                                website = page.locator(website_xpath).inner_text()
                                website = f"https://{website}" if website else ""
                                
                            if page.locator(phone_number_xpath).count() > 0:
                                phone_number = page.locator(phone_number_xpath).inner_text()
                                
                            if page.locator(place_type_xpath).count() > 0:
                                place_type = page.locator(place_type_xpath).inner_text()
                            
                            if page.locator(intro_xpath).count() > 0:
                                introduction = page.locator(intro_xpath).inner_text()
                            
                            if page.locator(reviews_count_xpath).count() > 0:
                                temp = page.locator(reviews_count_xpath).inner_text()
                                temp = temp.replace('(','').replace(')','').replace(',','')
                                try:
                                    review_count = int(temp)
                                except:
                                    review_count = ""

                            if page.locator(reviews_average_xpath).count() > 0:
                                temp = page.locator(reviews_average_xpath).inner_text()
                                temp = temp.replace(' ','').replace(',','.')
                                try:
                                    review_average = float(temp)
                                except:
                                    review_average = ""
                                    
                            # Process additional data for store info
                            # For info1
                            if page.locator(info1).count() > 0:
                                try:
                                    temp = page.locator(info1).inner_text(timeout=5000)  # Reduced timeout
                                    if '·' in temp:
                                        temp = temp.split('·')
                                        if len(temp) > 1:  # Make sure split was successful
                                            check = temp[1].replace("\n", "")
                                            if 'shop' in check:
                                                store_shopping = "Yes"
                                            elif 'pickup' in check:
                                                in_store_pickup = "Yes"
                                            elif 'delivery' in check:
                                                store_delivery = "Yes"
                                except Exception:
                                    store_shopping = "No"
                            else:
                                store_shopping = "No"

                            # Apply similar pattern for info2 and info3
                            if page.locator(info2).count() > 0:
                                try:
                                    temp = page.locator(info2).inner_text(timeout=5000)  # Reduced timeout
                                    if '·' in temp:
                                        temp = temp.split('·')
                                        if len(temp) > 1:  # Make sure split was successful
                                            check = temp[1].replace("\n", "")
                                            if 'pickup' in check:
                                                in_store_pickup = "Yes"
                                            elif 'shop' in check:
                                                store_shopping = "Yes"
                                            elif 'delivery' in check:
                                                store_delivery = "Yes"
                                except Exception:
                                    in_store_pickup = "No"
                            else:
                                in_store_pickup = "No"

                            if page.locator(info3).count() > 0:
                                try:
                                    temp = page.locator(info3).inner_text(timeout=5000)  # Reduced timeout
                                    if '·' in temp:
                                        temp = temp.split('·')
                                        if len(temp) > 1:  # Make sure split was successful
                                            check = temp[1].replace("\n", "")
                                            if 'Delivery' in check:
                                                store_delivery = "Yes"
                                            elif 'pickup' in check:
                                                in_store_pickup = "Yes"
                                            elif 'shop' in check:
                                                store_shopping = "Yes"
                                except Exception:
                                    store_delivery = "No"
                            else:
                                store_delivery = "No"
                            
                            if page.locator(opens_at_xpath).count() > 0:
                                opens = page.locator(opens_at_xpath).inner_text()
                                opens = opens.split('⋅')
                                if len(opens) != 1:
                                    opens = opens[1]
                                else:
                                    opens = page.locator(opens_at_xpath).inner_text()
                                opens = opens.replace("\u202f", "")
                                opens_at = opens
                            
                            # After extracting all the business data, extract reviews
                            if results_count < total:  # Only extract reviews if we're still collecting results
                                logger.info("Extracting reviews for this business...")
                                reviews = extract_reviews(page, name, address, max_reviews=10)
                                if reviews:
                                    save_reviews_to_csv(reviews)
                            
                            # Create record and save to CSV
                            record = {
                                'Names': name,
                                'Website': website,
                                'Introduction': introduction,
                                'Phone Number': phone_number,
                                'Address': address,
                                'Review Count': review_count,
                                'Average Review Count': review_average,
                                'Store Shopping': store_shopping,
                                'In Store Pickup': in_store_pickup,
                                'Delivery': store_delivery,
                                'Type': place_type,
                                'Opens At': opens_at
                            }
                            
                            # Append to CSV immediately and only increment count if successful
                            if append_to_csv(record):
                                results_count += 1
                                progress["results_count"] = results_count
                                logger.info(f"Processed {results_count}/{total} listings")
                            else:
                                logger.info(f"Duplicate skipped. Still at {results_count}/{total} listings")
                            
                            # Increment processed count
                            processed_count += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing URL {idx+1}: {e}")
                            error_count += 1
                    
                    # After processing all URLs for this grid cell, return to search for next cell
                    logger.info("Finished processing URLs for this grid cell")
                    
                except Exception as e:
                    logger.error(f"Failed to collect listings: {e}")
                
                # After processing all listings in a grid cell:

                # Log cell processing summary 
                logger.info(f"\n=== Grid Cell {cell_id} Summary ===")
                logger.info(f"Total listings found: {len(grid_listings)}")
                logger.info(f"Processed: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}")
                logger.info(f"Current progress: {results_count}/{total} unique listings collected")
                logger.info("=====================================\n")

                # Mark cell as completed
                completed_cells.append(cell_id)
                progress["completed_cells"] = completed_cells
                save_progress(progress)
                
                if results_count >= total:
                    break
                    
            except Exception as e:
                print(f"Error processing grid cell {cell_id}: {e}")
                # Continue to next cell on error
                continue
        
        # Finalize the CSV (remove duplicates if any)
        try:
            df = pd.read_csv('result.csv')
            df = df.drop_duplicates(subset=['Names', 'Address'])
            
            # Remove columns with only one unique value
            for column in df.columns:
                if df[column].nunique() == 1:
                    df.drop(column, axis=1, inplace=True)
                    
            df.to_csv('result.csv', index=False)
            print(f"Final dataset contains {len(df)} unique listings")
            print(df.head())
        except Exception as e:
            print(f"Error finalizing CSV: {e}")
        
        browser.close()
                            
    # Final deduplication of the CSV - using BOTH Name and Address
    try:
        print("Performing final deduplication of results...")
        df = pd.read_csv('result.csv')
        original_count = len(df)
        
        # Remove duplicates based on both name and address
        df = df.drop_duplicates(subset=['Names', 'Address'])
        
        # Save the deduplicated data
        df.to_csv('result.csv', index=False)
        print(f"Removed {original_count - len(df)} duplicate entries. Final dataset contains {len(df)} unique listings.")
    except Exception as e:
        print(f"Error during final deduplication: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str, help="Search term")
    parser.add_argument("-t", "--total", type=int, help="Total number of results to collect")
    parser.add_argument("-b", "--bounds", type=str, help="Search bounds in format 'min_lat,min_lng,max_lat,max_lng'")
    parser.add_argument("-g", "--grid", type=int, default=2, help="Grid size (default: 2x2)")
    args = parser.parse_args()
    
    search_for = args.search if args.search else "pharmacies in Germany"
    total = args.total if args.total else 50
    grid_size = args.grid
    
    if args.bounds:
        bounds = tuple(map(float, args.bounds.split(',')))
    else:
        # Default bounds - adjust these for your default area
        bounds = (43.6, -79.5, 43.9, -79.2)  # Default to Toronto area
    
    print(args)

    main()
