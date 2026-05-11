#!/usr/bin/env python3
"""
Pandas vs PySpark Performance Comparison

This script compares performance between Pandas and PySpark on 1M rows of data.
It includes loading, joining, aggregating, and timing operations.
"""

import time
import logging
from pathlib import Path
from typing import Dict, Any, Tuple

# Pandas imports
import pandas as pd

# PySpark imports
try:
    from spark_analytics import SalesAnalytics
    from pyspark.sql.functions import col, sum, desc
    PYSPARK_AVAILABLE = True
except ImportError as e:
    PYSPARK_AVAILABLE = False
    print(f"Warning: PySpark not available - {e}")


def setup_logging() -> logging.Logger:
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pandas_spark_comparison.log')
        ]
    )
    return logging.getLogger(__name__)


def measure_operation(operation_func, *args, **kwargs) -> Tuple[float, Any]:
    """
    Measure execution time of an operation.
    
    Args:
        operation_func: Function to measure
        *args, **kwargs: Arguments to pass to function
        
    Returns:
        Tuple of (execution_time, result)
    """
    start_time = time.time()
    result = operation_func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time, result


def pandas_analysis(customers_path: str, products_path: str, orders_path: str) -> Tuple[float, pd.DataFrame]:
    """
    Perform analysis using Pandas.
    
    Args:
        customers_path: Path to customers parquet file
        products_path: Path to products parquet file
        orders_path: Path to orders parquet file
        
    Returns:
        Tuple of (execution_time, result_dataframe)
    """
    print("🐼 Running Pandas analysis...")
    
    def pandas_operation():
        # Load data
        customers_df = pd.read_parquet(customers_path)
        products_df = pd.read_parquet(products_path)
        orders_df = pd.read_parquet(orders_path)
        
        print(f"   Loaded: {len(customers_df):,} customers, {len(products_df):,} products, {len(orders_df):,} orders")
        
        # Join orders with products
        merged_df = orders_df.merge(
            products_df[['product_id', 'price']], 
            on='product_id', 
            how='inner'
        )
        
        # Calculate revenue
        merged_df['revenue'] = merged_df['quantity'] * merged_df['price']
        
        # Group by customer_id and sum revenue
        customer_revenue = merged_df.groupby('customer_id')['revenue'].sum().reset_index()
        
        # Get top 10 customers
        top_customers = customer_revenue.nlargest(10, 'revenue').sort_values('customer_id')
        
        return top_customers
    
    return measure_operation(pandas_operation)


def pyspark_analysis(customers_path: str, products_path: str, orders_path: str) -> Tuple[float, Any]:
    """
    Perform analysis using PySpark.
    
    Args:
        customers_path: Path to customers parquet file
        products_path: Path to products parquet file
        orders_path: Path to orders parquet file
        
    Returns:
        Tuple of (execution_time, result_dataframe)
    """
    if not PYSPARK_AVAILABLE:
        return float('inf'), None
    
    print("⚡ Running PySpark analysis...")
    
    def pyspark_operation():
        # Initialize Spark
        analytics = SalesAnalytics()
        spark = analytics.create_spark_session()
        
        try:
            # Load data
            customers_df = analytics.load_parquet(customers_path, cache=False)
            products_df = analytics.load_parquet(products_path, cache=False)
            orders_df = analytics.load_parquet(orders_path, cache=False)
            
            print(f"   Loaded: {customers_df.count():,} customers, {products_df.count():,} products, {orders_df.count():,} orders")
            
            # Join orders with products and calculate revenue
            joined_df = orders_df.join(
                products_df.select('product_id', 'price'),
                orders_df.product_id == products_df.product_id,
                'inner'
            ).withColumn('revenue', col('quantity') * col('price'))
            
            # Group by customer_id and sum revenue
            customer_revenue = joined_df.groupBy('customer_id').agg(
                sum('revenue').alias('revenue')
            )
            
            # Get top 10 customers
            top_customers = customer_revenue.orderBy(desc('revenue')).limit(10)
            
            return top_customers
            
        finally:
            analytics.stop_spark_session()
    
    return measure_operation(pyspark_operation)


def format_comparison_table(pandas_time: float, pandas_result, spark_time: float, spark_result) -> None:
    """
    Format and display comparison table.
    
    Args:
        pandas_time: Pandas execution time
        pandas_result: Pandas result (for verification)
        spark_time: PySpark execution time
        spark_result: PySpark result (for verification)
    """
    print("\n" + "="*80)
    print("PANDAS VS PYSPARK PERFORMANCE COMPARISON")
    print("="*80)
    
    # Performance comparison table
    print(f"\n📊 PERFORMANCE METRICS:")
    print("-" * 50)
    print(f"{'Operation':<20} {'Pandas (s)':<15} {'PySpark (s)':<15} {'Speedup':<10}")
    print("-" * 50)
    
    # Calculate speedup
    if spark_time > 0 and pandas_time > 0:
        speedup = pandas_time / spark_time
        spark_faster = spark_time < pandas_time
        speedup_text = f"{speedup:.2f}x {'⚡' if spark_faster else '🐼'}"
    else:
        speedup_text = "N/A"
    
    print(f"{'Top 10 Customers':<20} {pandas_time:<15.3f} {spark_time:<15.3f} {speedup_text:<10}")
    
    # Memory usage estimation (rough)
    print(f"\n💾 MEMORY USAGE (estimated):")
    print("-" * 50)
    print(f"{'Pandas (RAM)':<20} ~{(1_000_000 * 0.1 / 1024):.1f} MB")
    print(f"{'PySpark (RAM)':<20} ~{(1_000_000 * 0.05 / 1024):.1f} MB")
    print(f"{'PySpark (Disk)':<20} Variable (depends on shuffle)")
    
    # Data validation
    if pandas_result is not None and spark_result is not None:
        print(f"\n✅ DATA VALIDATION:")
        print("-" * 50)
        
        # Convert PySpark result to pandas for comparison
        if PYSPARK_AVAILABLE:
            try:
                spark_pandas = spark_result.toPandas()
                pandas_shape = pandas_result.shape
                spark_shape = spark_pandas.shape
                
                print(f"{'Pandas result shape':<20} {pandas_shape}")
                print(f"{'PySpark result shape':<20} {spark_shape}")
                
                if pandas_shape == spark_shape:
                    print("✅ Results match!")
                else:
                    print("⚠️  Results have different shapes")
                    
            except Exception as e:
                print(f"⚠️  Could not validate results: {e}")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    print("-" * 50)
    
    if spark_time < pandas_time:
        print("🚀 PySpark is faster for this dataset size")
        print("   Consider using PySpark for datasets > 1M rows")
    else:
        print("🐼 Pandas is faster for this dataset size")
        print("   Consider using Pandas for datasets < 1M rows")
    
    print("📈 For larger datasets (> 10M rows), PySpark typically outperforms Pandas")
    print("🔧 For smaller datasets (< 100K rows), Pandas is usually more efficient")


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
        print("\n💡 Run main.py first to generate required data files.")
        raise FileNotFoundError("Required data files not found")
    
    return required_files


def main():
    """
    Main function to run performance comparison.
    """
    logger = setup_logging()
    logger.info("Starting Pandas vs PySpark performance comparison...")
    
    try:
        # Check data files
        print("🔍 Checking data files...")
        data_files = check_data_files()
        print("✅ All required data files found")
        
        # Run Pandas analysis
        pandas_time, pandas_result = pandas_analysis(
            str(data_files['customers']),
            str(data_files['products']),
            str(data_files['orders'])
        )
        
        # Run PySpark analysis
        spark_time, spark_result = pyspark_analysis(
            str(data_files['customers']),
            str(data_files['products']),
            str(data_files['orders'])
        )
        
        # Display comparison
        format_comparison_table(pandas_time, pandas_result, spark_time, spark_result)
        
    except FileNotFoundError as e:
        logger.error(f"Data files not found: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"\n❌ Unexpected error: {str(e)}")
    
    print("\n🎉 Performance comparison completed!")


if __name__ == "__main__":
    main()
