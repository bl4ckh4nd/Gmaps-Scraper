"""Geographic grid navigation for Google Maps scraper."""

from typing import List, Tuple, Optional
from dataclasses import dataclass
import logging

from ..utils.logger import get_component_logger


@dataclass
class GridCell:
    """Represents a single cell in the geographic grid."""
    
    id: str
    center_lat: float
    center_lng: float
    zoom: int
    min_lat: float
    min_lng: float
    max_lat: float
    max_lng: float
    
    def get_maps_url(self) -> str:
        """Generate Google Maps URL for this grid cell.
        
        Returns:
            Google Maps URL with coordinates and zoom
        """
        return f"https://www.google.com/maps/@{self.center_lat},{self.center_lng},{self.zoom}z"
    
    def contains_point(self, lat: float, lng: float) -> bool:
        """Check if a point is within this grid cell.
        
        Args:
            lat: Latitude
            lng: Longitude
            
        Returns:
            True if point is within this cell
        """
        return (self.min_lat <= lat <= self.max_lat and 
                self.min_lng <= lng <= self.max_lng)


class GridNavigator:
    """Handles geographic grid generation and navigation."""
    
    def __init__(self, bounds: Tuple[float, float, float, float], 
                 grid_size: int = 2, zoom_level: int = 12):
        """Initialize grid navigator.
        
        Args:
            bounds: Geographic bounds (min_lat, min_lng, max_lat, max_lng)
            grid_size: Grid dimensions (grid_size x grid_size cells)
            zoom_level: Zoom level for Google Maps
        """
        self.bounds = bounds
        self.grid_size = grid_size
        self.zoom_level = zoom_level
        self.logger = get_component_logger('GridNavigator')
        
        # Generate grid cells
        self.grid_cells = self._generate_grid()
        
        self.logger.info(f"Created {len(self.grid_cells)} grid cells "
                        f"({grid_size}x{grid_size}) with zoom {zoom_level}")
    
    def _generate_grid(self) -> List[GridCell]:
        """Generate a grid of cells within the specified bounds.
        
        Returns:
            List of GridCell instances
        """
        min_lat, min_lng, max_lat, max_lng = self.bounds
        lat_step = (max_lat - min_lat) / self.grid_size
        lng_step = (max_lng - min_lng) / self.grid_size
        
        grid_cells = []
        
        for i in range(self.grid_size):
            for j in range(self.grid_size):
                # Calculate cell boundaries
                cell_min_lat = min_lat + i * lat_step
                cell_max_lat = min_lat + (i + 1) * lat_step
                cell_min_lng = min_lng + j * lng_step
                cell_max_lng = min_lng + (j + 1) * lng_step
                
                # Calculate center point
                center_lat = cell_min_lat + lat_step / 2
                center_lng = cell_min_lng + lng_step / 2
                
                # Create cell ID
                cell_id = f"{i+1}_{j+1}"
                
                cell = GridCell(
                    id=cell_id,
                    center_lat=center_lat,
                    center_lng=center_lng,
                    zoom=self.zoom_level,
                    min_lat=cell_min_lat,
                    min_lng=cell_min_lng,
                    max_lat=cell_max_lat,
                    max_lng=cell_max_lng
                )
                
                grid_cells.append(cell)
        
        return grid_cells
    
    def get_cell_by_id(self, cell_id: str) -> Optional[GridCell]:
        """Get a grid cell by its ID.
        
        Args:
            cell_id: Cell identifier
            
        Returns:
            GridCell instance or None if not found
        """
        for cell in self.grid_cells:
            if cell.id == cell_id:
                return cell
        return None
    
    def get_cell_containing_point(self, lat: float, lng: float) -> Optional[GridCell]:
        """Get the grid cell containing a specific point.
        
        Args:
            lat: Latitude
            lng: Longitude
            
        Returns:
            GridCell instance or None if point is outside bounds
        """
        for cell in self.grid_cells:
            if cell.contains_point(lat, lng):
                return cell
        return None
    
    def get_neighboring_cells(self, cell: GridCell) -> List[GridCell]:
        """Get neighboring cells for a given cell.
        
        Args:
            cell: GridCell to find neighbors for
            
        Returns:
            List of neighboring GridCell instances
        """
        neighbors = []
        
        # Parse cell coordinates from ID
        try:
            i, j = map(int, cell.id.split('_'))
            i -= 1  # Convert to 0-based indexing
            j -= 1
            
            # Check all 8 possible neighbors
            for di in [-1, 0, 1]:
                for dj in [-1, 0, 1]:
                    if di == 0 and dj == 0:
                        continue  # Skip the cell itself
                    
                    ni, nj = i + di, j + dj
                    
                    # Check if neighbor is within grid bounds
                    if 0 <= ni < self.grid_size and 0 <= nj < self.grid_size:
                        neighbor_id = f"{ni+1}_{nj+1}"
                        neighbor = self.get_cell_by_id(neighbor_id)
                        if neighbor:
                            neighbors.append(neighbor)
                            
        except ValueError:
            self.logger.error(f"Invalid cell ID format: {cell.id}")
        
        return neighbors
    
    def get_total_area_km2(self) -> float:
        """Calculate total area covered by the grid in square kilometers.
        
        Returns:
            Area in square kilometers
        """
        min_lat, min_lng, max_lat, max_lng = self.bounds
        
        # Rough calculation (not accounting for Earth's curvature)
        lat_diff = max_lat - min_lat
        lng_diff = max_lng - min_lng
        
        # Convert to approximate km (1 degree ≈ 111 km)
        lat_km = lat_diff * 111
        lng_km = lng_diff * 111
        
        return lat_km * lng_km
    
    def get_cell_area_km2(self) -> float:
        """Calculate area of each grid cell in square kilometers.
        
        Returns:
            Area per cell in square kilometers
        """
        total_area = self.get_total_area_km2()
        total_cells = self.grid_size * self.grid_size
        return total_area / total_cells
    
    def get_progress_info(self, completed_cell_ids: List[str]) -> dict:
        """Get progress information for the grid.
        
        Args:
            completed_cell_ids: List of completed cell IDs
            
        Returns:
            Dictionary with progress information
        """
        total_cells = len(self.grid_cells)
        completed_count = len(completed_cell_ids)
        remaining_count = total_cells - completed_count
        progress_percentage = (completed_count / total_cells) * 100 if total_cells > 0 else 0
        
        return {
            'total_cells': total_cells,
            'completed_cells': completed_count,
            'remaining_cells': remaining_count,
            'progress_percentage': progress_percentage,
            'grid_size': f"{self.grid_size}x{self.grid_size}",
            'zoom_level': self.zoom_level,
            'bounds': self.bounds,
            'total_area_km2': self.get_total_area_km2(),
            'area_per_cell_km2': self.get_cell_area_km2()
        }
    
    def get_next_cell_to_process(self, completed_cell_ids: List[str]) -> Optional[GridCell]:
        """Get the next cell to process.
        
        Args:
            completed_cell_ids: List of already completed cell IDs
            
        Returns:
            Next GridCell to process or None if all completed
        """
        for cell in self.grid_cells:
            if cell.id not in completed_cell_ids:
                return cell
        return None
    
    def validate_bounds(self) -> bool:
        """Validate that the bounds are reasonable.
        
        Returns:
            True if bounds are valid
        """
        min_lat, min_lng, max_lat, max_lng = self.bounds
        
        # Check that bounds are in correct order
        if min_lat >= max_lat or min_lng >= max_lng:
            self.logger.error("Invalid bounds: min values must be less than max values")
            return False
        
        # Check that coordinates are within valid ranges
        if not (-90 <= min_lat <= 90) or not (-90 <= max_lat <= 90):
            self.logger.error("Invalid latitude values: must be between -90 and 90")
            return False
            
        if not (-180 <= min_lng <= 180) or not (-180 <= max_lng <= 180):
            self.logger.error("Invalid longitude values: must be between -180 and 180")
            return False
        
        # Check that the area isn't too large (could cause performance issues)
        area_km2 = self.get_total_area_km2()
        if area_km2 > 1000000:  # 1 million km²
            self.logger.warning(f"Large search area: {area_km2:.0f} km² - this may take a very long time")
        
        return True
    
    def __str__(self) -> str:
        """String representation of the grid navigator."""
        progress_info = self.get_progress_info([])
        return (f"GridNavigator({self.grid_size}x{self.grid_size}, "
                f"zoom={self.zoom_level}, "
                f"area={progress_info['total_area_km2']:.1f}km²)")
    
    def __repr__(self) -> str:
        """Detailed representation of the grid navigator."""
        return (f"GridNavigator(bounds={self.bounds}, "
                f"grid_size={self.grid_size}, "
                f"zoom_level={self.zoom_level}, "
                f"cells={len(self.grid_cells)})")