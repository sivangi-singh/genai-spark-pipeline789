#!/usr/bin/env python3
"""
PySpark Sales Analytics Module

This module provides comprehensive sales analytics capabilities using Apache Spark.
It includes methods for customer analysis, category performance, and trend analysis.
"""

import logging
from typing import Optional, Dict, Any
from pathlib import Path

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, count, sum, avg, max, min, round as spark_round,
        year, month, dayofmonth, desc, asc, lag,
        when, isnan, isnull, coalesce, lit
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
    from pyspark.storagelevel import StorageLevel
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    logging.error("PySpark not installed. Please install it using: pip install pyspark")


class SalesAnalytics:
    """
    A comprehensive PySpark analytics class for sales data analysis.
    
    This class provides methods to analyze sales data including:
    - Customer revenue analysis
    - Product category performance
    - Monthly trend analysis with growth calculations
    
    Attributes:
        spark: Configured SparkSession instance
        logger: Logger instance for tracking operations
    """
    
    def __init__(self, app_name: str = "SalesAnalytics", master: str = "local[*]") -> None:
        """
        Initialize the SalesAnalytics class.
        
        Args:
            app_name: Name for the Spark application
            master: Spark master URL (default: local[*] for local mode)
            
        Raises:
            ImportError: If PySpark is not installed
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required but not installed. Install with: pip install pyspark")
        
        self.app_name = app_name
        self.master = master
        self.logger = self._setup_logging()
        self.spark: Optional[SparkSession] = None
        
        self.logger.info("SalesAnalytics initialized")
    
    def _setup_logging(self) -> logging.Logger:
        """
        Setup logging configuration for the analytics class.
        
        Returns:
            Configured logger instance
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('sales_analytics.log')
            ]
        )
        return logging.getLogger(__name__)
    
    def create_spark_session(self, driver_memory: str = "4g", executor_memory: str = "4g") -> SparkSession:
        """
        Create and configure a SparkSession with optimal settings.
        
        Args:
            driver_memory: Driver memory allocation (default: 4GB)
            executor_memory: Executor memory allocation (default: 4GB)
            
        Returns:
            Configured SparkSession instance
            
        Raises:
            RuntimeError: If Spark session creation fails
        """
        try:
            self.logger.info("Creating SparkSession with optimized configuration...")
            
            # Build Spark session with comprehensive configuration
            builder = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master) \
                .config("spark.driver.memory", driver_memory) \
                .config("spark.executor.memory", executor_memory) \
                .config("spark.driver.cores", "4") \
                .config("spark.executor.cores", "4") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.kryo.registrationRequired", "false") \
                .config("spark.kryoserializer.buffer.max", "512m") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
                .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000") \
                .config("spark.default.parallelism", "8") \
                .config("spark.sql.broadcastTimeout", "36000")
            
            # Create session
            self.spark = builder.getOrCreate()
            
            # Set log level to reduce verbosity
            self.spark.sparkContext.setLogLevel("WARN")
            
            # Log configuration summary
            self.logger.info(f"SparkSession created successfully:")
            self.logger.info(f"  - App Name: {self.app_name}")
            self.logger.info(f"  - Driver Memory: {driver_memory}")
            self.logger.info(f"  - Executor Memory: {executor_memory}")
            self.logger.info(f"  - Adaptive Query Execution: Enabled")
            self.logger.info(f"  - Kryo Serialization: Enabled")
            
            return self.spark
            
        except Exception as e:
            self.logger.error(f"Failed to create SparkSession: {str(e)}")
            raise RuntimeError(f"Spark session creation failed: {str(e)}")
    
    def load_parquet(self, path: str, cache: bool = True) -> DataFrame:
        """
        Load parquet files into a Spark DataFrame.
        
        Args:
            path: Path to parquet file or directory
            cache: Whether to cache the DataFrame in memory
            
        Returns:
            Spark DataFrame containing the loaded data
            
        Raises:
            FileNotFoundError: If the specified path doesn't exist
            ValueError: If Spark session is not initialized
        """
        if self.spark is None:
            raise ValueError("Spark session not initialized. Call create_spark_session() first.")
        
        path_obj = Path(path)
        if not path_obj.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")
        
        try:
            self.logger.info(f"Loading parquet data from: {path}")
            
            # Load parquet file(s)
            df = self.spark.read.parquet(path)
            
            # Cache if requested
            if cache:
                df.cache()
                self.logger.info(f"DataFrame cached with {df.count():,} rows")
            
            # Log schema information
            self.logger.info(f"Loaded DataFrame schema: {df.schema}")
            self.logger.info(f"DataFrame shape: {df.count():,} rows, {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading parquet file: {str(e)}")
            raise
    
    def top_customers_by_revenue(self, orders_df: DataFrame, products_df: DataFrame, n: int = 10) -> DataFrame:
        """
        Calculate top customers by total revenue.
        
        This method joins orders with products, calculates total spend per customer,
        and returns the top N customers by revenue.
        
        Args:
            orders_df: DataFrame containing order data
            products_df: DataFrame containing product data
            n: Number of top customers to return (default: 10)
            
        Returns:
            DataFrame with top customers containing:
            - customer_id: Customer identifier
            - total_revenue: Total amount spent
            - order_count: Number of orders placed
            - avg_order_value: Average order value
            
        Raises:
            ValueError: If required columns are missing
        """
        self.logger.info(f"Calculating top {n} customers by revenue...")
        
        try:
            # Validate required columns
            required_order_cols = ['customer_id', 'product_id', 'quantity']
            required_product_cols = ['product_id', 'price']
            
            for col_name in required_order_cols:
                if col_name not in orders_df.columns:
                    raise ValueError(f"Missing required column in orders_df: {col_name}")
            
            for col_name in required_product_cols:
                if col_name not in products_df.columns:
                    raise ValueError(f"Missing required column in products_df: {col_name}")
            
            # Join orders with products to get price information
            joined_df = orders_df.join(
                products_df,
                orders_df.product_id == products_df.product_id,
                "inner"
            )
            
            # Calculate revenue per order line
            revenue_df = joined_df.withColumn(
                "revenue",
                col("quantity") * col("price")
            )
            
            # Aggregate by customer
            customer_revenue = revenue_df.groupBy("customer_id").agg(
                sum("revenue").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("revenue").alias("avg_order_value")
            )
            
            # Round numerical values
            customer_revenue = customer_revenue.withColumn(
                "total_revenue",
                spark_round(col("total_revenue"), 2)
            ).withColumn(
                "avg_order_value",
                spark_round(col("avg_order_value"), 2)
            )
            
            # Get top N customers by revenue
            top_customers = customer_revenue.orderBy(desc("total_revenue")).limit(n)
            
            self.logger.info(f"Successfully calculated top {n} customers")
            return top_customers
            
        except Exception as e:
            self.logger.error(f"Error calculating top customers by revenue: {str(e)}")
            raise
    
    def sales_by_category(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Calculate sales metrics by product category.
        
        This method joins orders with products, groups by category,
        and calculates total revenue and units sold per category.
        
        Args:
            orders_df: DataFrame containing order data
            products_df: DataFrame containing product data
            
        Returns:
            DataFrame with category sales containing:
            - category: Product category
            - total_revenue: Total revenue for the category
            - total_units_sold: Total units sold in the category
            - unique_products: Number of unique products in the category
            - avg_price_per_unit: Average price per unit
            
        Raises:
            ValueError: If required columns are missing
        """
        self.logger.info("Calculating sales by category...")
        
        try:
            # Validate required columns
            required_order_cols = ['product_id', 'quantity']
            required_product_cols = ['product_id', 'price', 'category']
            
            for col_name in required_order_cols:
                if col_name not in orders_df.columns:
                    raise ValueError(f"Missing required column in orders_df: {col_name}")
            
            for col_name in required_product_cols:
                if col_name not in products_df.columns:
                    raise ValueError(f"Missing required column in products_df: {col_name}")
            
            # Join orders with products
            joined_df = orders_df.join(
                products_df,
                orders_df.product_id == products_df.product_id,
                "inner"
            )
            
            # Calculate revenue per order line
            revenue_df = joined_df.withColumn(
                "revenue",
                col("quantity") * col("price")
            )
            
            # Aggregate by category
            category_sales = revenue_df.groupBy("category").agg(
                sum("revenue").alias("total_revenue"),
                sum("quantity").alias("total_units_sold"),
                countDistinct("product_id").alias("unique_products"),
                avg("price").alias("avg_price_per_unit")
            )
            
            # Round numerical values
            category_sales = category_sales.withColumn(
                "total_revenue",
                spark_round(col("total_revenue"), 2)
            ).withColumn(
                "avg_price_per_unit",
                spark_round(col("avg_price_per_unit"), 2)
            )
            
            # Sort by total revenue descending
            category_sales = category_sales.orderBy(desc("total_revenue"))
            
            self.logger.info("Successfully calculated sales by category")
            return category_sales
            
        except Exception as e:
            self.logger.error(f"Error calculating sales by category: {str(e)}")
            raise
    
    def monthly_trends(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Calculate monthly revenue trends with month-over-month growth.
        
        This method joins orders with products, calculates monthly revenue,
        and uses Window functions to calculate month-over-month growth percentage.
        
        Args:
            orders_df: DataFrame containing order data
            products_df: DataFrame containing product data
            
        Returns:
            DataFrame with monthly trends containing:
            - year: Year of the period
            - month: Month of the period
            - monthly_revenue: Total revenue for the month
            - monthly_growth_pct: Month-over-month growth percentage
            - revenue_change: Absolute change in revenue from previous month
            
        Raises:
            ValueError: If required columns are missing
        """
        self.logger.info("Calculating monthly revenue trends...")
        
        try:
            # Validate required columns
            required_order_cols = ['product_id', 'quantity', 'order_date']
            required_product_cols = ['product_id', 'price']
            
            for col_name in required_order_cols:
                if col_name not in orders_df.columns:
                    raise ValueError(f"Missing required column in orders_df: {col_name}")
            
            for col_name in required_product_cols:
                if col_name not in products_df.columns:
                    raise ValueError(f"Missing required column in products_df: {col_name}")
            
            # Join orders with products
            joined_df = orders_df.join(
                products_df,
                orders_df.product_id == products_df.product_id,
                "inner"
            )
            
            # Calculate revenue and extract year/month
            revenue_df = joined_df.withColumn(
                "revenue",
                col("quantity") * col("price")
            ).withColumn(
                "year",
                year("order_date")
            ).withColumn(
                "month",
                month("order_date")
            )
            
            # Aggregate monthly revenue
            monthly_revenue = revenue_df.groupBy("year", "month").agg(
                sum("revenue").alias("monthly_revenue")
            ).orderBy("year", "month")
            
            # Define window specification for month-over-month calculation
            window_spec = Window.orderBy("year", "month").rowsBetween(-1, -1)
            
            # Calculate month-over-month growth using Window functions
            monthly_trends = monthly_revenue.withColumn(
                "prev_month_revenue",
                lag("monthly_revenue", 1).over(window_spec)
            ).withColumn(
                "revenue_change",
                coalesce(col("monthly_revenue") - col("prev_month_revenue"), lit(0))
            ).withColumn(
                "monthly_growth_pct",
                when(
                    col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") > 0),
                    spark_round(
                        ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100,
                        2
                    )
                ).otherwise(lit(None))
            )
            
            # Round revenue values
            monthly_trends = monthly_trends.withColumn(
                "monthly_revenue",
                spark_round(col("monthly_revenue"), 2)
            ).withColumn(
                "revenue_change",
                spark_round(col("revenue_change"), 2)
            )
            
            # Select final columns and order
            final_trends = monthly_trends.select(
                "year", "month", "monthly_revenue", 
                "monthly_growth_pct", "revenue_change"
            ).orderBy("year", "month")
            
            self.logger.info("Successfully calculated monthly revenue trends")
            return final_trends
            
        except Exception as e:
            self.logger.error(f"Error calculating monthly trends: {str(e)}")
            raise
    
    def run_complete_analysis(self, 
                            orders_path: str, 
                            products_path: str,
                            top_customers_n: int = 10) -> Dict[str, DataFrame]:
        """
        Run complete sales analysis pipeline.
        
        This method loads data and runs all analysis methods.
        
        Args:
            orders_path: Path to orders parquet file
            products_path: Path to products parquet file
            top_customers_n: Number of top customers to return
            
        Returns:
            Dictionary containing all analysis results:
            - top_customers: Top customers by revenue
            - category_sales: Sales by category
            - monthly_trends: Monthly revenue trends
        """
        self.logger.info("Starting complete sales analysis...")
        
        if self.spark is None:
            self.create_spark_session()
        
        try:
            # Load data
            orders_df = self.load_parquet(orders_path)
            products_df = self.load_parquet(products_path)
            
            # Run all analyses
            results = {
                'top_customers': self.top_customers_by_revenue(orders_df, products_df, top_customers_n),
                'category_sales': self.sales_by_category(orders_df, products_df),
                'monthly_trends': self.monthly_trends(orders_df, products_df)
            }
            
            self.logger.info("Complete sales analysis finished successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in complete analysis: {str(e)}")
            raise
    
    def stop_spark_session(self) -> None:
        """Stop the Spark session and clean up resources."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            self.logger.info("Spark session stopped")
    
    def __del__(self) -> None:
        """Cleanup when object is destroyed."""
        self.stop_spark_session()


def main():
    """
    Main function to demonstrate SalesAnalytics usage.
    """
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize analytics
        analytics = SalesAnalytics()
        
        # Create Spark session
        spark = analytics.create_spark_session()
        
        # Example usage (assuming parquet files exist)
        print("SalesAnalytics initialized successfully!")
        print("Spark session created with optimized configuration.")
        print("\nTo run analysis, use:")
        print("  analytics.run_complete_analysis('path/to/orders.parquet', 'path/to/products.parquet')")
        
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
