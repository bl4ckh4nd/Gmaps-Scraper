"""XPath and CSS selectors for Google Maps scraping."""

from dataclasses import dataclass
from typing import List


@dataclass
class Selectors:
    """Container for all XPath and CSS selectors used in scraping."""
    
    # Search and navigation selectors
    SEARCH_INPUT: str = '//input[@id="searchboxinput"]'
    SEARCH_INPUT_SELECTORS: List[str] = None
    SEARCH_RESULTS: str = '//a[contains(@href, "https://www.google.com/maps/place")]'
    RESULTS_FEED: str = '[role="feed"]'
    
    # Business information selectors
    BUSINESS_NAME: str = '//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]'
    BUSINESS_ADDRESS: str = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
    BUSINESS_WEBSITE: str = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
    BUSINESS_PHONE: str = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
    BUSINESS_TYPE: str = '//div[@class="LBgpqf"]//button[@class="DkEaL "]'
    BUSINESS_INTRO: str = '//div[@class="WeS02d fontBodyMedium"]//div[@class="PYvSYb "]'
    
    # Review selectors
    REVIEWS_COUNT: str = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span//span//span[@aria-label]'
    REVIEWS_AVERAGE: List[str] = None
    RATING_SELECTOR: str = '//span[@role="img" and contains(@class, "ceNzKf") and contains(@aria-label, "Sterne")]'
    
    # Operating hours selectors  
    OPENS_AT: str = '//button[contains(@data-item-id, "oh")]//div[contains(@class, "fontBodyMedium")]'
    OPENS_AT_ALT: str = '//div[@class="MkV9"]//span[@class="ZDu9vd"]//span[2]'
    
    # Service type selectors
    INFO1: str = '//div[@class="LTs0Rc"][1]'
    INFO2: str = '//div[@class="LTs0Rc"][2]'  
    INFO3: str = '//div[@class="LTs0Rc"][3]'
    
    # Review tab and content selectors
    REVIEWS_TAB_SELECTORS: List[str] = None
    REVIEW_CONTAINERS: str = 'xpath=//div[@class="jftiEf fontBodyMedium "]'
    
    # Individual review element selectors
    REVIEWER_NAME_SELECTORS: List[str] = None
    REVIEW_TEXT_SELECTORS: List[str] = None
    REVIEW_STARS_SELECTORS: List[str] = None
    REVIEW_DATE_SELECTORS: List[str] = None
    OWNER_RESPONSE_SELECTORS: List[str] = None
    
    # Scrollable feed selectors for reviews
    REVIEW_FEED_SELECTORS: List[str] = None
    
    # Cookie consent banner selectors
    COOKIE_BANNER_SELECTORS: List[str] = None
    REJECT_ALL_BUTTON_SELECTORS: List[str] = None
    ACCEPT_ALL_BUTTON_SELECTORS: List[str] = None
    
    def __post_init__(self):
        """Initialize list selectors after dataclass creation."""
        self.REVIEWS_AVERAGE = [
            '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[1]/span[1]',
            '//div[@class="F7nice"]//span[1]/span[1]',
            '//div[@class="fontBodyMedium dmRWX"]//span[@aria-hidden and contains(text(), ",")]'
        ]
        
        self.REVIEWS_TAB_SELECTORS = [
            'xpath=//button[@role="tab" and contains(@aria-label, "Rezensionen")]',
            'xpath=//button[@role="tab" and contains(@aria-label, "Reviews")]',
            'xpath=//button[@role="tab"]//div[contains(text(), "Rezensionen")]/..',
            'xpath=//button[@role="tab"]//div[contains(text(), "Reviews")]/..',
            'xpath=//button[@role="tab" and @data-tab-index="1"]',
            'xpath=//button[@jsaction="pane.rating.moreReviews"]'
        ]
        
        self.REVIEWER_NAME_SELECTORS = [
            'xpath=.//div[contains(@class, "d4r55")]',
            'css=div.d4r55',
            '.d4r55'
        ]
        
        self.REVIEW_TEXT_SELECTORS = [
            'xpath=.//div[@class="MyEned"]//span[@class="wiI7pd"]',
            'css=div.MyEned span.wiI7pd',
            '.wiI7pd'
        ]
        
        self.REVIEW_STARS_SELECTORS = [
            'xpath=.//span[@class="kvMYJc"]',
            'css=span.kvMYJc',
            '.kvMYJc'
        ]
        
        self.REVIEW_DATE_SELECTORS = [
            'xpath=.//span[@class="rsqaWe"]',
            'css=span.rsqaWe', 
            '.rsqaWe'
        ]
        
        self.OWNER_RESPONSE_SELECTORS = [
            'xpath=.//div[@class="CDe7pd"]//div[@class="wiI7pd"]',
            'css=div.CDe7pd div.wiI7pd',
            'div.CDe7pd .wiI7pd'
        ]
        
        self.REVIEW_FEED_SELECTORS = [
            'xpath=//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[3]',
            '#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde',
            'div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde',
            'div[role="feed"]',
            '.m6QErb'
        ]
        
        self.COOKIE_BANNER_SELECTORS = [
            'div[aria-label*="cookie"]',
            'div[aria-label*="Cookie"]',
            'div[class*="consent"]',
            'div[role="dialog"]',
            'form[data-google-cookie-consent]',
            '[data-consent-banner]',
            'div[jscontroller*="consent"]',
            # Updated selectors based on actual Google consent form structure
            'form:has(button.UywwFc-LgbsSe)',
            'div:has(button[aria-label*="ablehnen"])',
            'div:has(button[aria-label*="akzeptieren"])',
            '[jscontroller]:has(button.UywwFc-LgbsSe)',
            'body:has(button[aria-label="Alle ablehnen"])',
            'form[data-consent]',
            '[data-gs-st]:has(button.UywwFc-LgbsSe)'
        ]
        
        self.REJECT_ALL_BUTTON_SELECTORS = [
            # Direct class and aria-label selectors based on debug logs
            'button.UywwFc-LgbsSe[aria-label="Alle ablehnen"]',
            'button[aria-label="Alle ablehnen"]',
            'button[aria-label="Reject all"]',
            'button[aria-label="Tout refuser"]',
            # Class-based selectors for Google consent form
            'button.UywwFc-LgbsSe:has-text("Alle ablehnen")',
            'button.XWZjwc[aria-label*="ablehnen"]',
            # Original selectors as fallbacks
            'button:has-text("Alle ablehnen")',
            'button:has-text("Reject all")',
            'button:has-text("Tout refuser")',
            'button:has-text("Rechazar todo")',
            'button[aria-label*="Alle ablehnen"]',
            'button[aria-label*="Reject all"]',
            'button[data-value="0"]',
            '//button[contains(text(), "Alle ablehnen")]',
            '//button[contains(text(), "Reject all")]',
            '//button[contains(text(), "Tout refuser")]',
            # Aggressive fallbacks
            'button:text-matches(".*ablehnen.*", "i")',
            'button:text-matches(".*reject.*", "i")'
        ]
        
        self.ACCEPT_ALL_BUTTON_SELECTORS = [
            # Direct class and aria-label selectors based on debug logs
            'button.UywwFc-LgbsSe[aria-label="Alle akzeptieren"]',
            'button[aria-label="Alle akzeptieren"]',
            'button[aria-label="Accept all"]',
            'button[aria-label="Tout accepter"]',
            # Class-based selectors for Google consent form
            'button.UywwFc-LgbsSe:has-text("Alle akzeptieren")',
            'button.XWZjwc[aria-label*="akzeptieren"]',
            # Original selectors as fallbacks
            'button:has-text("Alle akzeptieren")',
            'button:has-text("Accept all")',
            'button:has-text("Tout accepter")',
            'button:has-text("Aceptar todo")',
            'button[aria-label*="Alle akzeptieren"]',
            'button[aria-label*="Accept all"]',
            'button[data-value="1"]',
            '//button[contains(text(), "Alle akzeptieren")]',
            '//button[contains(text(), "Accept all")]',
            '//button[contains(text(), "Tout accepter")]',
            # Aggressive fallbacks
            'button:text-matches(".*akzeptieren.*", "i")',
            'button:text-matches(".*accept.*", "i")'
        ]
        
        self.SEARCH_INPUT_SELECTORS = [
            '//input[@id="searchboxinput"]',
            'input#searchboxinput',
            'input[aria-label*="Search"]',
            'input[aria-label*="Suche"]', 
            'input[aria-label*="Recherche"]',
            'input[placeholder*="Search"]',
            'input[placeholder*="Suche"]',
            '//input[@name="q"]',
            'input.searchboxinput',
            '[data-value="Search"]',
            'input[jsaction*="search"]',
            '//input[contains(@class, "searchboxinput")]'
        ]