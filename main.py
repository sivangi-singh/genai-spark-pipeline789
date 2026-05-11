#!/usr/bin/env python3
"""
Main script for E-Commerce Data Pipeline

This script orchestrates the generation of synthetic e-commerce data and saves it as Parquet files.
It includes comprehensive error handling, timing, and file size reporting.
"""

import time
import os
import logging
from pathlib import Path
from typing import Dict, Any

# Import our custom modules
from src.data_generator import SyntheticDataGenerator
from src.config import config


def setup_logging() -> logging.Logger:
    """
    Setup logging configuration for the main script.
    
    Returns:
        Configured logger instance
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline_main.log')
        ]
    )
    return logging.getLogger(__name__)


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Formatted file size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"


def save_as_parquet(data_dict: Dict[str, Any], output_dir: str) -> Dict[str, str]:
    """
    Save DataFrames as Parquet files.
    
    Args:
        data_dict: Dictionary containing DataFrames to save
        output_dir: Directory to save Parquet files
        
    Returns:
        Dictionary with file paths and sizes
    """
    logger = logging.getLogger(__name__)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    file_info = {}
    
    for name, df in data_dict.items():
        file_path = output_path / f"{name}.parquet"
        
        try:
            # Save as Parquet
            df.to_parquet(file_path, index=False, engine='pyarrow')
            
            # Get file size
            file_size = file_path.stat().st_size
            
            file_info[name] = {
                'path': str(file_path),
                'size_bytes': file_size,
                'size_formatted': format_file_size(file_size)
            }
            
            logger.info(f"Saved {name} to {file_path} ({file_info[name]['size_formatted']})")
            
        except Exception as e:
            logger.error(f"Error saving {name} to Parquet: {str(e)}")
            raise
    
    return file_info


def main() -> None:
    """
    Main function to orchestrate the data generation pipeline.
    """
    logger = setup_logging()
    logger.info("Starting E-Commerce Data Pipeline")
    
    try:
        # Record start time
        start_time = time.time()
        
        # Initialize data generator
        logger.info("Initializing SyntheticDataGenerator...")
        generator = SyntheticDataGenerator(random_seed=42)
        
        # Get configuration values
        num_customers = config.default_customers
        num_products = config.default_products
        num_orders = config.default_orders
        
        logger.info(f"Configuration: {num_customers:,} customers, {num_products:,} products, {num_orders:,} orders")
        
        # Generate data
        logger.info("Starting data generation...")
        data = generator.generate_all_data(
            num_customers=num_customers,
            num_products=num_products,
            num_orders=num_orders,
            save_to_files=False  # We'll save as Parquet manually
        )
        
        # Save as Parquet files
        logger.info("Saving data as Parquet files...")
        output_dir = config.raw_data_dir
        file_info = save_as_parquet(data, str(output_dir))
        
        # Calculate generation time
        end_time = time.time()
        generation_time = end_time - start_time
        
        # Print summary
        print("\n" + "="*60)
        print("DATA GENERATION COMPLETED SUCCESSFULLY")
        print("="*60)
        
        print(f"\nGeneration Time: {generation_time:.2f} seconds ({generation_time/60:.2f} minutes)")
        
        print("\nGenerated Data:")
        print(f"  Customers: {len(data['customers']):,}")
        print(f"  Products:  {len(data['products']):,}")
        print(f"  Orders:    {len(data['orders']):,}")
        
        print("\nSaved Files:")
        total_size = 0
        for name, info in file_info.items():
            print(f"  {name}.parquet: {info['size_formatted']}")
            total_size += info['size_bytes']
        
        print(f"\nTotal Storage: {format_file_size(total_size)}")
        print(f"Output Directory: {output_dir}")
        
        # Print data samples
        print("\nData Samples:")
        print("\nCustomers (first 3):")
        print(data['customers'].head(3).to_string(index=False))
        
        print("\nProducts (first 3):")
        print(data['products'].head(3).to_string(index=False))
        
        print("\nOrders (first 3):")
        print(data['orders'].head(3).to_string(index=False))
        
        logger.info("Pipeline completed successfully")
        
    except ImportError as e:
        logger.error(f"Import error: {str(e)}")
        print(f"\nERROR: Missing dependency - {str(e)}")
        print("Please install required packages: pip install -r requirements.txt")
        
    except FileNotFoundError as e:
        logger.error(f"File not found error: {str(e)}")
        print(f"\nERROR: File not found - {str(e)}")
        print("Please ensure all required directories exist.")
        
    except PermissionError as e:
        logger.error(f"Permission error: {str(e)}")
        print(f"\nERROR: Permission denied - {str(e)}")
        print("Please check file/directory permissions.")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"\nERROR: An unexpected error occurred - {str(e)}")
        print("Please check the logs for more details.")
        
    finally:
        logger.info("Pipeline execution finished")


if __name__ == "__main__":
    main()
