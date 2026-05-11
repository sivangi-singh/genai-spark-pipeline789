"""
Configuration settings for the E-Commerce Data Pipeline.

This module contains all configuration parameters used throughout the pipeline,
including data generation settings, Spark configuration, and file paths.
"""

import os
import logging
from typing import Dict, Any
from pathlib import Path


class Config:
    """Configuration class for the E-Commerce Data Pipeline."""
    
    def __init__(self) -> None:
        """Initialize configuration with default values."""
        # Base paths
        self.base_dir: Path = Path(__file__).parent.parent
        self.data_dir: Path = self.base_dir / "data"
        self.raw_data_dir: Path = self.data_dir / "raw"
        self.processed_data_dir: Path = self.data_dir / "processed"
        
        # Ensure directories exist
        self._ensure_directories()
        
        # Data generation settings
        self.default_customers: int = 1000
        self.default_products: int = 500
        self.default_orders: int = 5000
        
        # Product categories
        self.product_categories: list[str] = [
            "Electronics", "Clothing", "Books", "Home & Garden", 
            "Sports", "Toys", "Food", "Beauty", "Automotive", "Health"
        ]
        
        # Customer age ranges
        self.age_ranges: Dict[str, tuple[int, int]] = {
            "young": (18, 25),
            "adult": (26, 45),
            "middle": (46, 65),
            "senior": (66, 80)
        }
        
        # Price ranges for products
        self.price_ranges: Dict[str, tuple[float, float]] = {
            "Electronics": (50.0, 2000.0),
            "Clothing": (10.0, 200.0),
            "Books": (5.0, 50.0),
            "Home & Garden": (20.0, 500.0),
            "Sports": (15.0, 300.0),
            "Toys": (5.0, 100.0),
            "Food": (1.0, 50.0),
            "Beauty": (10.0, 150.0),
            "Automotive": (25.0, 1000.0),
            "Health": (5.0, 200.0)
        }
        
        # File formats
        self.output_format: str = "csv"  # Options: csv, json, parquet
        
        # Spark configuration
        self.spark_config: Dict[str, Any] = {
            "spark.app.name": "E-Commerce Analytics",
            "spark.master": "local[*]",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g"
        }
        
        # Logging configuration
        self.log_level: str = "INFO"
        self.log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        self.log_file: str = str(self.base_dir / "pipeline.log")
        
        # Initialize logging
        self._setup_logging()
    
    def _ensure_directories(self) -> None:
        """Ensure all required directories exist."""
        directories = [
            self.data_dir,
            self.raw_data_dir,
            self.processed_data_dir
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logging.info(f"Ensured directory exists: {directory}")
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper()),
            format=self.log_format,
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        logging.info("Logging configuration initialized")
    
    def get_file_path(self, data_type: str, filename: str = None) -> Path:
        """
        Get the appropriate file path for a given data type.
        
        Args:
            data_type: Type of data ('raw' or 'processed')
            filename: Optional filename. If not provided, a default will be generated.
            
        Returns:
            Path object for the file
        """
        if data_type == "raw":
            base_path = self.raw_data_dir
        elif data_type == "processed":
            base_path = self.processed_data_dir
        else:
            raise ValueError("data_type must be 'raw' or 'processed'")
        
        if filename is None:
            filename = f"{data_type}_data.{self.output_format}"
        
        return base_path / filename
    
    def get_spark_config(self) -> Dict[str, Any]:
        """
        Get Spark configuration dictionary.
        
        Returns:
            Dictionary containing Spark configuration
        """
        return self.spark_config.copy()


# Global configuration instance
config = Config()
