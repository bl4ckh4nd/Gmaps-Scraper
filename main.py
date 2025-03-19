from playwright.sync_api import sync_playwright
from dataclasses import dataclass, asdict, field
import pandas as pd
import argparse

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
            grid_points.append((center_lat, center_lng, 16))
    
    return grid_points

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path='C:\Program Files\Google\Chrome\Application\chrome.exe', headless=False)
        page = browser.new_page()
        
        # Store all results across all grid cells
        all_listings = []
        seen_urls = set()  # For deduplication - CHANGED from seen_names
        
        # Define the search area bounds (you can customize these)
        # Format: (min_latitude, min_longitude, max_latitude, max_longitude)
        search_bounds = bounds
        
        # Generate grid points
        grid_points = generate_grid(search_bounds, grid_size=grid_size)
        
        print(f"Split search into {len(grid_points)} grid areas")
        
        # For each grid point, perform a search
        for i, (lat, lng, zoom) in enumerate(grid_points):
            print(f"Searching grid cell {i+1}/{len(grid_points)}...")
            
            # Go to Google Maps with specific coordinates and zoom level
            page.goto(f"https://www.google.com/maps/@{lat},{lng},{zoom}z", timeout=60000)
            page.wait_for_timeout(1000)
            
            # Perform the search
            page.locator('//input[@id="searchboxinput"]').fill(search_for)
            page.keyboard.press("Enter")
            page.wait_for_selector('//a[contains(@href, "https://www.google.com/maps/place")]', timeout=10000)
            
            # (Rest of your existing scrolling code)
            page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')
            
            # Your existing scroll logic
            results_selector = '[role="feed"]'
            previously_counted = 0
            max_attempts = 5
            static_count_attempts = 0
            
            print("Starting to scroll for results...")
            
            while static_count_attempts < max_attempts:
                current_count = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                print(f"Currently Found: {current_count}")
                
                if current_count >= min(120, total):  # Get up to 120 per grid cell or the total if lower
                    print(f"Found {current_count} results in this grid cell")
                    break
                    
                # Your existing scrolling logic
                if current_count == previously_counted:
                    static_count_attempts += 1
                    print(f"No new results found. Attempt {static_count_attempts}/{max_attempts}")
                else:
                    static_count_attempts = 0
                    previously_counted = current_count
                
                if page.locator(results_selector).count() > 0:
                    page.evaluate("""(args) => {
                        const element = document.querySelector(args.selector);
                        if (element) {
                            element.scrollTop = element.scrollHeight * 0.5 * args.multiplier;
                        }
                    }""", {"selector": results_selector, "multiplier": static_count_attempts + 1})
                else:
                    page.mouse.wheel(0, 15000)
                
                page.wait_for_timeout(3000)
            
            # Get the listings for this grid cell
            grid_listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()
            
            # Process listings
            for listing in grid_listings:
                try:
                    # Extract URL for deduplication check - CHANGED
                    url = listing.get_attribute('href')
                    
                    # Skip if we've already seen this listing
                    if url in seen_urls:
                        continue
                    
                    # Mark as seen
                    seen_urls.add(url)
                    
                    # Now click and process
                    listing_elem = listing.locator("xpath=..")
                    listing_elem.click()
                    
                    # Verify we can access the listing details
                    page.wait_for_selector('//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]', timeout=5000)
                    
                    # Add to our collection
                    all_listings.append(listing_elem)
                    
                    # Stop if we've reached our total target
                    if len(all_listings) >= total:
                        break
                except Exception as e:
                    print(f"Error processing listing: {e}")
            
            # Break out of grid search if we have enough results
            if len(all_listings) >= total:
                print(f"Reached target of {total} unique listings across {i+1} grid cells")
                break
        
        # Now process the collected unique listings (rest of your code)
        print(f"Total unique listings found: {len(all_listings)}")
        
        # Reset lists for data collection
        names_list.clear()
        address_list.clear()
        website_list.clear()
        phones_list.clear()
        reviews_c_list.clear()
        reviews_a_list.clear()
        open_list.clear()
        intro_list.clear()
        store_s_list.clear()
        in_store_list.clear()
        store_del_list.clear()
        place_t_list.clear()
        
        # Process the listings as in your original code
        for listing in all_listings[:total]:
            # Your existing code for processing each listing
            listing.click()
            page.wait_for_selector('//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]')
            
            name_xpath = '//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]'
            address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
            website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
            phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
            reviews_count_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span//span//span[@aria-label]'
            reviews_average_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span[@aria-hidden and contains(text(), ",")]'
            
            info1='//div[@class="LTs0Rc"][1]'#store
            info2='//div[@class="LTs0Rc"][2]'#pickup
            info3='//div[@class="LTs0Rc"][3]'#delivery
            opens_at_xpath='//button[contains(@data-item-id, "oh")]//div[contains(@class, "fontBodyMedium")]'#time
            opens_at_xpath2='//div[@class="MkV9"]//span[@class="ZDu9vd"]//span[2]'
            place_type_xpath='//div[@class="LBgpqf"]//button[@class="DkEaL "]'#type of place
            intro_xpath='//div[@class="WeS02d fontBodyMedium"]//div[@class="PYvSYb "]'
            print("Scraping...")
          
            
            if page.locator(intro_xpath).count() > 0:
                Introduction = page.locator(intro_xpath).inner_text()
                intro_list.append(Introduction)
            else:
                Introduction = ""
                intro_list.append("None Found")
            
            if page.locator(reviews_count_xpath).count() > 0:
                temp = page.locator(reviews_count_xpath).inner_text()
                temp=temp.replace('(','').replace(')','').replace(',','')
                Reviews_Count=int(temp)
                reviews_c_list.append(Reviews_Count)
            else:
                Reviews_Count = ""
                reviews_c_list.append(Reviews_Count)

            if page.locator(reviews_average_xpath).count() > 0:
                temp = page.locator(reviews_average_xpath).inner_text()
                temp=temp.replace(' ','').replace(',','.')
                Reviews_Average=float(temp)
                reviews_a_list.append(Reviews_Average)
            else:
                Reviews_Average = ""
                reviews_a_list.append(Reviews_Average)


            # Fix for info1, info2, and info3 sections

            # For info1
            if page.locator(info1).count() > 0:
                try:
                    temp = page.locator(info1).inner_text(timeout=5000)  # Reduced timeout
                    if '·' in temp:
                        temp = temp.split('·')
                        if len(temp) > 1:  # Make sure split was successful
                            check = temp[1].replace("\n", "")
                            if 'shop' in check:
                                Store_Shopping = check
                                store_s_list.append("Yes")
                            elif 'pickup' in check:
                                In_Store_Pickup = check
                                in_store_list.append("Yes")
                            elif 'delivery' in check:
                                Store_Delivery = check
                                store_del_list.append("Yes")
                            else:
                                store_s_list.append("No")  # Default if none match
                        else:
                            store_s_list.append("No")
                    else:
                        store_s_list.append("No")
                except Exception:
                    store_s_list.append("No")
                    Store_Shopping = ""
            else:
                Store_Shopping = ""
                store_s_list.append("No")

            # Apply similar pattern for info2 and info3
            if page.locator(info2).count() > 0:
                try:
                    temp = page.locator(info2).inner_text(timeout=5000)  # Reduced timeout
                    if '·' in temp:
                        temp = temp.split('·')
                        if len(temp) > 1:  # Make sure split was successful
                            check = temp[1].replace("\n", "")
                            if 'pickup' in check:
                                In_Store_Pickup = check
                                in_store_list.append("Yes")
                            elif 'shop' in check:
                                Store_Shopping = check
                                store_s_list.append("Yes")
                            elif 'delivery' in check:
                                Store_Delivery = check
                                store_del_list.append("Yes")
                            else:
                                in_store_list.append("No")  # Default if none match
                        else:
                            in_store_list.append("No")
                    else:
                        in_store_list.append("No")
                except Exception:
                    in_store_list.append("No")
                    In_Store_Pickup = ""
            else:
                In_Store_Pickup = ""
                in_store_list.append("No")

            if page.locator(info3).count() > 0:
                try:
                    temp = page.locator(info3).inner_text(timeout=5000)  # Reduced timeout
                    if '·' in temp:
                        temp = temp.split('·')
                        if len(temp) > 1:  # Make sure split was successful
                            check = temp[1].replace("\n", "")
                            if 'Delivery' in check:
                                Store_Delivery = check
                                store_del_list.append("Yes")
                            elif 'pickup' in check:
                                In_Store_Pickup = check
                                in_store_list.append("Yes")
                            elif 'shop' in check:
                                Store_Shopping = check
                                store_s_list.append("Yes")
                            else:
                                store_del_list.append("No")  # Default if none match
                        else:
                            store_del_list.append("No")
                    else:
                        store_del_list.append("No")
                except Exception:
                    store_del_list.append("No")
                    Store_Delivery = ""
            else:
                Store_Delivery = ""
                store_del_list.append("No")
            

            if page.locator(opens_at_xpath).count() > 0:
                opens = page.locator(opens_at_xpath).inner_text()
                
                opens=opens.split('⋅')
                
                if len(opens)!=1:
                    opens=opens[1]
               
                else:
                    opens = page.locator(opens_at_xpath).inner_text()
                    # print(opens)
                opens=opens.replace("\u202f","")
                Opens_At=opens
                open_list.append(Opens_At)
               
            else:
                Opens_At = ""
                open_list.append(Opens_At)
            if page.locator(opens_at_xpath2).count() > 0:
                opens = page.locator(opens_at_xpath2).inner_text()
                
                opens=opens.split('⋅')
                opens=opens[1]
                opens=opens.replace("\u202f","")
                Opens_At=opens
                open_list.append(Opens_At)

            extract_data(name_xpath, names_list, page)
            extract_data(address_xpath, address_list, page)
            extract_data(website_xpath, website_list, page)
            extract_data(phone_number_xpath, phones_list, page)
            extract_data(place_type_xpath, place_t_list, page)
            
  
        # Create the DataFrame from your collected data
        df = pd.DataFrame(list(zip(names_list, website_list,intro_list,phones_list,address_list,reviews_c_list,reviews_a_list,store_s_list,in_store_list,store_del_list,place_t_list,open_list)), columns =['Names','Website','Introduction','Phone Number','Address','Review Count','Average Review Count','Store Shopping','In Store Pickup','Delivery','Type','Opens At'])
        
        # Add "https://" prefix to non-empty website URLs
        df['Website'] = df['Website'].apply(lambda x: f"https://{x}" if x else x)
        
        # Remove columns with only one unique value
        for column in df.columns:
            if df[column].nunique() == 1:
                df.drop(column, axis=1, inplace=True)
                
        df.to_csv(r'result.csv', index = False)
        browser.close()
        print(df.head())



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str, help="Search term")
    parser.add_argument("-t", "--total", type=int, help="Total number of results to collect")
    parser.add_argument("-b", "--bounds", type=str, help="Search bounds in format 'min_lat,min_lng,max_lat,max_lng'")
    parser.add_argument("-g", "--grid", type=int, default=2, help="Grid size (default: 2x2)")
    args = parser.parse_args()
    print(args)

    search_for = args.search if args.search else "pharmacies in Germany"
    total = args.total if args.total else 50
    
    if args.bounds:
        bounds = tuple(map(float, args.bounds.split(',')))
    else:
        # Default bounds - adjust these for your default area
        bounds = (43.6, -79.5, 43.9, -79.2)  # Default to Toronto area
        
    grid_size = args.grid

    main()
