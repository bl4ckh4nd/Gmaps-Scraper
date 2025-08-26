#!/usr/bin/env python3
"""Test script to verify both scraping modes work correctly."""

import math
from src.google_maps_scraper import GoogleMapsScraper
from src.config import Config


def test_mode_calculation():
    """Test the mathematical logic for both modes."""
    print("=== Testing Mode Calculation Logic ===")
    
    # Test case: 4x4 grid with 500 target results
    grid_size = 4
    total_results = 500
    total_cells = grid_size * grid_size
    max_per_cell = 120  # Default from config
    
    print(f"Grid: {grid_size}x{grid_size} = {total_cells} cells")
    print(f"Target results: {total_results}")
    print(f"Max per cell: {max_per_cell}")
    print()
    
    # Fast mode simulation
    print("FAST MODE:")
    print("- Processes cells sequentially")
    print("- Stops when target reached")
    cells_needed_fast = min(total_cells, math.ceil(total_results / max_per_cell))
    print(f"- Expected cells used: ~{cells_needed_fast} (until {total_results} results collected)")
    print()
    
    # Coverage mode simulation  
    print("COVERAGE MODE:")
    print("- Processes ALL cells with distributed target")
    results_per_cell = math.ceil(total_results / total_cells)
    print(f"- Target per cell: {results_per_cell}")
    print(f"- Expected cells used: {total_cells} (all cells)")
    print(f"- Expected final count: {total_cells} * {results_per_cell} = {total_cells * results_per_cell} results")
    print()


def test_config_loading():
    """Test that configuration loads correctly with new mode setting."""
    print("=== Testing Configuration Loading ===")
    
    try:
        config = Config.from_file('config.yaml')
        print(f"[OK] Default scraping mode: {config.settings.scraping.default_mode}")
        print(f"[OK] Max listings per cell: {config.settings.scraping.max_listings_per_cell}")
        print(f"[OK] Configuration loaded successfully")
    except Exception as e:
        print(f"[ERROR] Configuration loading failed: {e}")
    print()


def test_scraper_initialization():
    """Test that scraper initializes with both modes."""
    print("=== Testing Scraper Initialization ===")
    
    try:
        config = Config.from_file('config.yaml')
        scraper = GoogleMapsScraper(config)
        print("[OK] Scraper initialized successfully")
        
        # Test method signature for run()
        import inspect
        sig = inspect.signature(scraper.run)
        params = list(sig.parameters.keys())
        if 'scraping_mode' in params:
            print("[OK] run() method supports scraping_mode parameter")
        else:
            print("[ERROR] run() method missing scraping_mode parameter")
            
    except Exception as e:
        print(f"[ERROR] Scraper initialization failed: {e}")
    print()


def simulate_grid_processing():
    """Simulate how each mode would process a grid."""
    print("=== Simulating Grid Processing ===")
    
    # Simulation parameters
    grid_size = 4
    total_results = 500
    total_cells = grid_size * grid_size
    max_per_cell = 120
    
    # Simulate uneven distribution (some cells have more results)
    simulated_results_per_cell = [
        45, 38, 52, 41,  # Row 1: Total = 176
        48, 35, 59, 43,  # Row 2: Total = 185  
        41, 46, 33, 49,  # Row 3: Total = 169
        37, 44, 40, 38   # Row 4: Total = 159
    ]
    
    print(f"Simulated results per cell: {simulated_results_per_cell}")
    print(f"Total available results: {sum(simulated_results_per_cell)}")
    print()
    
    # Fast mode simulation
    print("FAST MODE SIMULATION:")
    fast_collected = 0
    fast_cells_used = 0
    for i, cell_results in enumerate(simulated_results_per_cell):
        if fast_collected >= total_results:
            break
        
        # Take up to remaining needed or available in cell
        take = min(cell_results, total_results - fast_collected)
        fast_collected += take
        fast_cells_used += 1
        print(f"  Cell {i+1}: took {take}/{cell_results} results (total: {fast_collected})")
    
    print(f"Fast mode result: {fast_collected} results from {fast_cells_used}/{total_cells} cells")
    print()
    
    # Coverage mode simulation
    print("COVERAGE MODE SIMULATION:")
    results_per_cell_target = math.ceil(total_results / total_cells)
    coverage_collected = 0
    coverage_cells_used = 0
    
    for i, cell_results in enumerate(simulated_results_per_cell):
        # Take up to the fair share from each cell
        take = min(cell_results, results_per_cell_target)
        coverage_collected += take
        coverage_cells_used += 1
        print(f"  Cell {i+1}: took {take}/{cell_results} results (target: {results_per_cell_target})")
    
    print(f"Coverage mode result: {coverage_collected} results from {coverage_cells_used}/{total_cells} cells")
    print()
    
    # Analysis
    print("ANALYSIS:")
    print(f"Fast mode used {fast_cells_used}/{total_cells} cells ({fast_cells_used/total_cells*100:.1f}%)")
    print(f"Coverage mode used {coverage_cells_used}/{total_cells} cells ({coverage_cells_used/total_cells*100:.1f}%)")
    print(f"Geographic coverage improvement: {(coverage_cells_used - fast_cells_used)} additional cells")


def main():
    """Run all tests."""
    print("Google Maps Scraper - Dual Mode Testing")
    print("=" * 50)
    print()
    
    test_mode_calculation()
    test_config_loading()
    test_scraper_initialization() 
    simulate_grid_processing()
    
    print("=" * 50)
    print("Testing completed!")
    print()
    print("To test with actual data:")
    print("1. Start the web interface: python web/app.py")
    print("2. Try both modes with a small search (e.g., 20 results, 2x2 grid)")
    print("3. Compare the cell distribution in the job progress")


if __name__ == "__main__":
    main()