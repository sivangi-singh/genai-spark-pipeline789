#!/usr/bin/env python3
"""
Analytics Runner Script

This script orchestrates the complete sales analytics pipeline using SalesAnalytics.
It loads data, runs all analysis methods, and displays results with timing.
"""

import time
import logging
from pathlib import Path
from typing import Dict, Any

# Import SalesAnalytics from spark_analytics
try:
    from spark_analytics import SalesAnalytics
except ImportError as e:
    print(f"Error importing SalesAnalytics: {e}")
    print("Make sure spark_analytics.py is in the current directory")
    exit(1)


def setup_logging() -> logging.Logger:
    """
    Setup logging configuration for the analytics runner.
    
    Returns:
        Configured logger instance
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('run_analytics.log')
        ]
    )
    return logging.getLogger(__name__)


def measure_operation_time(operation_func, *args, **kwargs) -> tuple[float, Any]:
    """
    Measure execution time of an operation.
    
    Args:
        operation_func: Function to measure
        *args, **kwargs: Arguments to pass to the function
        
    Returns:
        Tuple of (execution_time, result)
    """
    start_time = time.time()
    result = operation_func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time, result


def display_results(results: Dict[str, Any], execution_times: Dict[str, float]) -> None:
    """
    Display analysis results with execution times.
    
    Args:
        results: Dictionary containing analysis results
        execution_times: Dictionary containing execution times for each operation
    """
    print("\n" + "="*80)
    print("ANALYTICS RESULTS")
    print("="*80)
    
    # Display top customers
    if 'top_customers' in results:
        print(f"\n🏆 TOP CUSTOMERS BY REVENUE (took {execution_times['top_customers']:.2f}s)")
        print("-" * 50)
        results['top_customers'].show(10, truncate=False)
    
    # Display sales by category
    if 'category_sales' in results:
        print(f"\n📊 SALES BY CATEGORY (took {execution_times['category_sales']:.2f}s)")
        print("-" * 50)
        results['category_sales'].show(truncate=False)
    
    # Display monthly trends
    if 'monthly_trends' in results:
        print(f"\n📈 MONTHLY REVENUE TRENDS (took {execution_times['monthly_trends']:.2f}s)")
        print("-" * 50)
        results['monthly_trends'].show(20, truncate=False)
    
    # Display execution summary
    print("\n" + "="*80)
    print("EXECUTION SUMMARY")
    print("="*80)
    
    total_time = sum(execution_times.values())
    
    for operation, exec_time in execution_times.items():
        percentage = (exec_time / total_time) * 100
        print(f"{operation.replace('_', ' ').title():<25}: {exec_time:>8.2f}s ({percentage:>5.1f}%)")
    
    print(f"{'Total Execution Time':<25}: {total_time:>8.2f}s (100.0%)")


def check_data_files() -> Dict[str, Path]:
    """
    Check if required data files exist.
    
    Returns:
        Dictionary with file paths
        
    Raises:
        FileNotFoundError: If required files don't exist
    """
    data_dir = Path("data/raw")
    required_files = {
        'customers': data_dir / "customers.parquet",
        'products': data_dir / "products.parquet", 
        'orders': data_dir / "orders.parquet"
    }
    
    missing_files = []
    for name, path in required_files.items():
        if not path.exists():
            missing_files.append(str(path))
    
    if missing_files:
        print("❌ Missing required data files:")
        for file_path in missing_files:
            print(f"   - {file_path}")
        print("\n💡 Run main.py first to generate the required data files.")
        raise FileNotFoundError("Required data files not found")
    
    return required_files


def main():
    """
    Main function to run the complete analytics pipeline.
    """
    logger = setup_logging()
    logger.info("Starting analytics pipeline...")
    
    try:
        # Check if data files exist
        print("🔍 Checking data files...")
        data_files = check_data_files()
        print("✅ All required data files found")
        
        # Initialize SalesAnalytics
        print("\n🚀 Initializing SalesAnalytics...")
        analytics = SalesAnalytics()
        
        # Create Spark session
        print("🔥 Creating Spark session...")
        spark = analytics.create_spark_session()
        print("✅ Spark session created successfully")
        
        # Load data files
        print("\n📂 Loading data files...")
        load_start = time.time()
        
        customers_df = analytics.load_parquet(str(data_files['customers']))
        products_df = analytics.load_parquet(str(data_files['products']))
        orders_df = analytics.load_parquet(str(data_files['orders']))
        
        load_time = time.time() - load_start
        print(f"✅ Data loaded in {load_time:.2f} seconds")
        
        # Run analytics with timing
        print("\n📊 Running analytics...")
        results = {}
        execution_times = {}
        
        # Top customers analysis
        print("   🔍 Analyzing top customers by revenue...")
        exec_time, results['top_customers'] = measure_operation_time(
            analytics.top_customers_by_revenue, orders_df, products_df, n=10
        )
        execution_times['top_customers'] = exec_time
        print(f"   ✅ Completed in {exec_time:.2f} seconds")
        
        # Sales by category analysis
        print("   📈 Analyzing sales by category...")
        exec_time, results['category_sales'] = measure_operation_time(
            analytics.sales_by_category, orders_df, products_df
        )
        execution_times['category_sales'] = exec_time
        print(f"   ✅ Completed in {exec_time:.2f} seconds")
        
        # Monthly trends analysis
        print("   📅 Analyzing monthly revenue trends...")
        exec_time, results['monthly_trends'] = measure_operation_time(
            analytics.monthly_trends, orders_df, products_df
        )
        execution_times['monthly_trends'] = exec_time
        print(f"   ✅ Completed in {exec_time:.2f} seconds")
        
        # Display results
        display_results(results, execution_times)
        
        # Save results to files (optional)
        save_results = input("\n💾 Save results to parquet files? (y/n): ").lower().strip()
        if save_results == 'y':
            save_analytics_results(results, analytics)
        
    except FileNotFoundError as e:
        logger.error(f"Data files not found: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        
    except ImportError as e:
        logger.error(f"Import error: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        print("Make sure PySpark is installed: pip install pyspark")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"\n❌ Unexpected error: {str(e)}")
        
    finally:
        # Always stop Spark session
        try:
            if 'analytics' in locals():
                print("\n🛑 Stopping Spark session...")
                analytics.stop_spark_session()
                print("✅ Spark session stopped")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {str(e)}")
    
    print("\n🎉 Analytics pipeline completed!")


def save_analytics_results(results: Dict[str, Any], analytics) -> None:
    """
    Save analytics results to parquet files.
    
    Args:
        results: Dictionary containing analysis results
        analytics: SalesAnalytics instance
    """
    try:
        output_dir = Path("data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n💾 Saving results to {output_dir}...")
        
        for name, df in results.items():
            output_path = output_dir / f"{name}.parquet"
            df.write.mode("overwrite").parquet(str(output_path))
            print(f"   ✅ Saved {name} to {output_path}")
            
    except Exception as e:
        print(f"❌ Error saving results: {str(e)}")


if __name__ == "__main__":
    main()
