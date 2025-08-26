# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Google Maps web scraper built with Python and Playwright that extracts business information and reviews. The application has been **fully refactored** from a monolithic 1056-line script into a modular, object-oriented architecture with clear separation of concerns.

## New Modular Architecture (v2.0)

**Entry Points:**
- `main_new.py` - New modular entry point with comprehensive argument parsing
- `main.py` - Legacy monolithic version (deprecated)

**Core Package Structure:**
```
src/
├── __init__.py                    # Package initialization
├── google_maps_scraper.py         # Main orchestrator class
├── config/                        # Configuration management
│   ├── settings.py                # Settings classes and Config loader
│   └── selectors.py               # All XPath/CSS selectors
├── models/                        # Data models
│   ├── business.py                # Business dataclass with validation
│   └── review.py                  # Review dataclass with validation
├── scraper/                       # Scraping components
│   ├── base_scraper.py            # Abstract base with common functionality
│   ├── business_scraper.py        # Business data extraction
│   └── review_scraper.py          # Review extraction with scrolling
├── navigation/                    # Navigation and page interaction
│   ├── grid_navigator.py          # Geographic grid generation/management
│   └── page_navigator.py          # Browser navigation and interaction
├── persistence/                   # Data persistence layer
│   ├── csv_writer.py              # CSV operations with deduplication
│   └── progress_tracker.py        # Job state management
└── utils/                         # Utilities and helpers
    ├── exceptions.py              # Custom exception hierarchy
    ├── helpers.py                 # Utility functions (place_id extraction, etc.)
    └── logger.py                  # Logging configuration and adapters
```

**Configuration System:**
- `config.yaml` - External configuration file for all settings
- Environment variable support
- Hierarchical configuration with validation
- Separate settings for browser, scraping, grid, and file operations

## Dependencies and Setup

**Python Version Requirement:**
- README specifies Python < 3.10 (versions beyond 3.9 may cause issues)
- Current system runs Python 3.12.2 - may need version management

**Key Dependencies (from requirements.txt):**
- `playwright>=1.33.0` - Web automation and browser control
- `pandas` - Data manipulation and CSV operations  
- `numpy` - Numerical operations support
- `openpyxl>=3.1.1` - Excel file support

**Installation:**
```bash
pip install -r requirements.txt
playwright install chromium  # Install browser
```

## Usage Commands

**New Modular Version (Recommended):**
```bash
python main_new.py -s "search term" -t total_results
```

**Examples:**
```bash
# Basic usage
python main_new.py -s "restaurants in Toronto" -t 50

# With custom grid and bounds
python main_new.py -s "Turkish Restaurants" -t 100 -b "43.6,-79.5,43.9,-79.2" -g 3

# With custom configuration
python main_new.py -s "pharmacies in Germany" -t 200 --config my_config.yaml --headless true
```

**Parameters:**
- `-s/--search`: Search term (required)
- `-t/--total`: Target number of results (required)
- `-b/--bounds`: Geographic bounds as "min_lat,min_lng,max_lat,max_lng" (optional)
- `-g/--grid`: Grid size for geographic division (default: 2x2)
- `--config`: Configuration file path (default: config.yaml)
- `--headless`: Override headless browser setting
- `--max-reviews`: Override max reviews per business
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

**Legacy Version:**
```bash
python main.py -s "search term" -t total_results -b "bounds" -g grid_size
```

## Data Architecture

**Business Data Schema (`result.csv`):**
- Place ID, Names, Website, Introduction, Phone Number, Address
- Maps URL, Review Count, Average Review  
- Store Shopping, In Store Pickup, Delivery, Type, Opens At

**Review Data Schema (`reviews.csv`):**
- place_id, business_name, business_address
- reviewer_name, review_text, rating, review_date
- owner_response, language (auto-detected)

## Progress Tracking System

The scraper uses JSON files to track progress and enable job resumption:
- `scraper_progress.json` - Current job state
- `scraper_progress_[search_term].json` - Named job states
- Contains: completed_cells, seen_urls, results_count, search parameters

**Resume interrupted jobs:** Simply re-run the same command - the scraper automatically detects and continues from where it left off.

## Browser Configuration

**Chrome Path:** Hardcoded to `C:\Program Files\Google\Chrome\Application\chrome.exe`
- Runs in non-headless mode (visible browser)
- Uses sync_playwright for synchronous operation

## Error Handling and Logging

**Logging System:**
- Timestamped log files: `scraper_log_YYYYMMDD_HHMMSS.log`
- Both file and console output
- Comprehensive error tracking and progress reporting

**Resilience Features:**
- Duplicate detection via place_id extraction
- Fallback selectors for different Google Maps layouts
- Grid cell error recovery (continues to next cell on failure)
- Batch processing for review extraction

## Geographic Grid System

**Grid Logic:**
- Divides search bounds into `grid_size x grid_size` cells
- Each cell searched independently with specific coordinates
- Default zoom level 12 for optimal result density
- Processes up to 120 listings per cell (configurable)

## Review Extraction Details

**Review Processing:**
- Extracts up to 100 reviews per business (configurable)
- Handles scrolling for review pagination
- Multiple selector fallbacks for robustness
- Language detection (German/English)
- Batch saving (10 reviews per batch)

**Review Selectors:**
- Uses XPath and CSS selectors with fallbacks
- Handles both German and English Google Maps interfaces
- Robust scrolling mechanism for loading more reviews

## Output Files

**Generated Files:**
- `result.csv` - Main business data
- `reviews.csv` - Detailed review data  
- `result_YYYYMMDDHHMISS.csv` - Timestamped backups
- `scraper_log_*.log` - Execution logs
- `scraper_progress*.json` - Job state files

## Performance Considerations

**Timing Configuration:**
- 1.5s scroll intervals (configurable)
- 2s wait times for page loads
- 30s timeout for page navigation
- 5-10s timeouts for element selection

**Limits:**
- Max 5 scroll attempts when no new results found
- 120 listings per grid cell (prevents infinite loops)
- 10 review batches for continuous saving

## Development and Testing

**Code Organization:**
- Each component has a single responsibility
- Abstract base classes provide common functionality
- Dependency injection for easy testing and mocking
- Clear separation between business logic and I/O operations

**Testing Framework:**
```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests (when implemented)
python -m pytest tests/
python -m pytest tests/ --cov=src/  # With coverage
```

**Testing Approach:**
1. Run small searches (5-10 results) to verify functionality
2. Check both CSV outputs are generated correctly  
3. Verify progress tracking works by interrupting and resuming jobs
4. Test different geographic bounds and search terms
5. Unit test individual components in isolation

**Code Quality:**
```bash
# Format code
black src/ tests/

# Lint code  
flake8 src/ tests/
```

## Migration from Legacy Version

**Key Differences:**
- **Entry Point**: Use `main_new.py` instead of `main.py`
- **Configuration**: Settings moved to `config.yaml` instead of hardcoded values
- **Error Handling**: Better exception handling and recovery
- **Logging**: Structured logging with component identification  
- **Performance**: Better memory usage and resource management

**Migration Steps:**
1. Install new dependencies: `pip install -r requirements.txt`
2. Create/customize `config.yaml` configuration file
3. Update scripts to use `main_new.py` instead of `main.py`
4. Test with small datasets before large scraping jobs
5. Monitor logs for any configuration adjustments needed