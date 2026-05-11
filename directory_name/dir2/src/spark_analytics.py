"""
PySpark Analytics Module for E-Commerce Pipeline

This module provides comprehensive analytics capabilities for e-commerce data using Apache Spark.
It includes various business insights and analysis functions.
"""

import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
import pandas as pd

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, count, sum, avg, max, min, datediff, current_date,
        year, month, dayofweek, hour, rank, desc, asc,
        when, round as spark_round, countDistinct
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
except ImportError:
    logging.error("PySpark not installed. Please install it using: pip install pyspark")
    raise

from .config import config


class SparkAnalytics:
    """
    A class to perform analytics on e-commerce data using PySpark.
    
    This class provides methods for:
    - Sales analysis and revenue trends
    - Customer segmentation and behavior analysis
    - Product performance analysis
    - Time-based analysis
    """
    
    def __init__(self) -> None:
        """Initialize SparkAnalytics with Spark session and configuration."""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # DataFrames
        self.customers_df: Optional[DataFrame] = None
        self.products_df: Optional[DataFrame] = None
        self.orders_df: Optional[DataFrame] = None
        self.order_items_df: Optional[DataFrame] = None
        
        self.logger.info("SparkAnalytics initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.
        
        Returns:
            Configured SparkSession
        """
        spark_config = self.config.get_spark_config()
        
        spark = SparkSession.builder \
            .appName(spark_config.get("spark.app.name", "E-Commerce Analytics")) \
            .master(spark_config.get("spark.master", "local[*]")) \
            .config("spark.sql.adaptive.enabled", spark_config.get("spark.sql.adaptive.enabled", "true")) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", 
                   spark_config.get("spark.sql.adaptive.coalescePartitions.enabled", "true")) \
            .config("spark.driver.memory", spark_config.get("spark.driver.memory", "2g")) \
            .config("spark.executor.memory", spark_config.get("spark.executor.memory", "2g")) \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info("Spark session created successfully")
        return spark
    
    def load_data(self) -> None:
        """Load all data from CSV files into Spark DataFrames."""
        self.logger.info("Loading data from files")
        
        try:
            # Load customers
            customers_path = self.config.get_file_path('raw', 'customers.csv')
            if customers_path.exists():
                self.customers_df = self.spark.read.csv(str(customers_path), header=True, inferSchema=True)
                self.logger.info(f"Loaded customers data from {customers_path}")
            
            # Load products
            products_path = self.config.get_file_path('raw', 'products.csv')
            if products_path.exists():
                self.products_df = self.spark.read.csv(str(products_path), header=True, inferSchema=True)
                self.logger.info(f"Loaded products data from {products_path}")
            
            # Load orders
            orders_path = self.config.get_file_path('raw', 'orders.csv')
            if orders_path.exists():
                self.orders_df = self.spark.read.csv(str(orders_path), header=True, inferSchema=True)
                self.logger.info(f"Loaded orders data from {orders_path}")
            
            # Load order items
            order_items_path = self.config.get_file_path('raw', 'order_items.csv')
            if order_items_path.exists():
                self.order_items_df = self.spark.read.csv(str(order_items_path), header=True, inferSchema=True)
                self.logger.info(f"Loaded order items data from {order_items_path}")
                
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
    
    def analyze_sales_overview(self) -> DataFrame:
        """
        Analyze overall sales metrics.
        
        Returns:
            DataFrame with sales overview metrics
        """
        self.logger.info("Analyzing sales overview")
        
        if self.orders_df is None:
            raise ValueError("Orders data not loaded. Call load_data() first.")
        
        sales_overview = self.orders_df.agg(
            count("order_id").alias("total_orders"),
            sum("order_total").alias("total_revenue"),
            avg("order_total").alias("avg_order_value"),
            max("order_total").alias("max_order_value"),
            min("order_total").alias("min_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        )
        
        return sales_overview
    
    def analyze_monthly_sales_trends(self) -> DataFrame:
        """
        Analyze monthly sales trends.
        
        Returns:
            DataFrame with monthly sales data
        """
        self.logger.info("Analyzing monthly sales trends")
        
        if self.orders_df is None:
            raise ValueError("Orders data not loaded. Call load_data() first.")
        
        monthly_sales = self.orders_df.withColumn("year", year("order_date")) \
            .withColumn("month", month("order_date")) \
            .groupBy("year", "month") \
            .agg(
                count("order_id").alias("order_count"),
                sum("order_total").alias("total_revenue"),
                avg("order_total").alias("avg_order_value")
            ) \
            .orderBy("year", "month")
        
        return monthly_sales
    
    def analyze_top_products(self, limit: int = 10) -> DataFrame:
        """
        Analyze top-selling products.
        
        Args:
            limit: Number of top products to return
            
        Returns:
            DataFrame with top products by revenue
        """
        self.logger.info(f"Analyzing top {limit} products")
        
        if self.order_items_df is None or self.products_df is None:
            raise ValueError("Order items or products data not loaded. Call load_data() first.")
        
        top_products = self.order_items_df.join(
            self.products_df,
            self.order_items_df.product_id == self.products_df.product_id,
            "inner"
        ).groupBy(
            self.products_df.product_id,
            self.products_df.product_name,
            self.products_df.category
        ).agg(
            sum(self.order_items_df.quantity).alias("total_quantity_sold"),
            sum(self.order_items_df.total_price).alias("total_revenue"),
            count(self.order_items_df.order_id).alias("order_count")
        ).orderBy(desc("total_revenue")).limit(limit)
        
        return top_products
    
    def analyze_customer_segments(self) -> DataFrame:
        """
        Analyze customer segments based on RFM (Recency, Frequency, Monetary) analysis.
        
        Returns:
            DataFrame with customer segments
        """
        self.logger.info("Analyzing customer segments")
        
        if self.orders_df is None or self.customers_df is None:
            raise ValueError("Orders or customers data not loaded. Call load_data() first.")
        
        # Calculate RFM metrics
        customer_rfm = self.orders_df.groupBy("customer_id").agg(
            max("order_date").alias("last_order_date"),
            count("order_id").alias("frequency"),
            sum("order_total").alias("monetary")
        ).withColumn(
            "recency_days",
            datediff(current_date(), col("last_order_date"))
        )
        
        # Create segments based on RFM scores
        customer_segments = customer_rfm.withColumn(
            "recency_score",
            when(col("recency_days") <= 30, 5)
            .when(col("recency_days") <= 60, 4)
            .when(col("recency_days") <= 90, 3)
            .when(col("recency_days") <= 180, 2)
            .otherwise(1)
        ).withColumn(
            "frequency_score",
            when(col("frequency") >= 10, 5)
            .when(col("frequency") >= 7, 4)
            .when(col("frequency") >= 5, 3)
            .when(col("frequency") >= 3, 2)
            .otherwise(1)
        ).withColumn(
            "monetary_score",
            when(col("monetary") >= 1000, 5)
            .when(col("monetary") >= 500, 4)
            .when(col("monetary") >= 200, 3)
            .when(col("monetary") >= 100, 2)
            .otherwise(1)
        ).withColumn(
            "rfm_score",
            col("recency_score") + col("frequency_score") + col("monetary_score")
        ).withColumn(
            "segment",
            when(col("rfm_score") >= 13, "Champions")
            .when(col("rfm_score") >= 10, "Loyal Customers")
            .when(col("rfm_score") >= 7, "Potential Loyalists")
            .when(col("rfm_score") >= 5, "At Risk")
            .otherwise("Lost")
        )
        
        return customer_segments
    
    def analyze_category_performance(self) -> DataFrame:
        """
        Analyze product category performance.
        
        Returns:
            DataFrame with category performance metrics
        """
        self.logger.info("Analyzing category performance")
        
        if self.order_items_df is None or self.products_df is None:
            raise ValueError("Order items or products data not loaded. Call load_data() first.")
        
        category_performance = self.order_items_df.join(
            self.products_df,
            self.order_items_df.product_id == self.products_df.product_id,
            "inner"
        ).groupBy(self.products_df.category).agg(
            sum(self.order_items_df.quantity).alias("total_quantity_sold"),
            sum(self.order_items_df.total_price).alias("total_revenue"),
            countDistinct(self.order_items_df.product_id).alias("unique_products"),
            count(self.order_items_df.order_id).alias("order_count")
        ).orderBy(desc("total_revenue"))
        
        return category_performance
    
    def analyze_hourly_sales_pattern(self) -> DataFrame:
        """
        Analyze hourly sales patterns.
        
        Returns:
            DataFrame with hourly sales data
        """
        self.logger.info("Analyzing hourly sales patterns")
        
        if self.orders_df is None:
            raise ValueError("Orders data not loaded. Call load_data() first.")
        
        hourly_sales = self.orders_df.withColumn("hour", hour("order_date")) \
            .groupBy("hour") \
            .agg(
                count("order_id").alias("order_count"),
                sum("order_total").alias("total_revenue"),
                avg("order_total").alias("avg_order_value")
            ) \
            .orderBy("hour")
        
        return hourly_sales
    
    def run_full_analysis(self) -> Dict[str, DataFrame]:
        """
        Run complete analytics pipeline.
        
        Returns:
            Dictionary containing all analysis results
        """
        self.logger.info("Starting full analytics pipeline")
        
        # Load data if not already loaded
        if self.orders_df is None:
            self.load_data()
        
        # Run all analyses
        results = {
            'sales_overview': self.analyze_sales_overview(),
            'monthly_sales_trends': self.analyze_monthly_sales_trends(),
            'top_products': self.analyze_top_products(),
            'customer_segments': self.analyze_customer_segments(),
            'category_performance': self.analyze_category_performance(),
            'hourly_sales_pattern': self.analyze_hourly_sales_pattern()
        }
        
        # Save results to files
        self._save_results(results)
        
        self.logger.info("Full analytics pipeline completed")
        return results
    
    def _save_results(self, results: Dict[str, DataFrame]) -> None:
        """
        Save analysis results to files.
        
        Args:
            results: Dictionary containing analysis results
        """
        self.logger.info("Saving analysis results")
        
        for name, df in results.items():
            output_path = self.config.get_file_path('processed', f'{name}.csv')
            df.toPandas().to_csv(output_path, index=False)
            self.logger.info(f"Saved {name} results to {output_path}")
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        Get summary statistics of the loaded data.
        
        Returns:
            Dictionary containing summary statistics
        """
        if self.orders_df is None:
            self.load_data()
        
        stats = {}
        
        if self.customers_df:
            stats['customers'] = self.customers_df.count()
        
        if self.products_df:
            stats['products'] = self.products_df.count()
        
        if self.orders_df:
            stats['orders'] = self.orders_df.count()
        
        if self.order_items_df:
            stats['order_items'] = self.order_items_df.count()
        
        return stats
    
    def stop_spark_session(self) -> None:
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")
    
    def __del__(self) -> None:
        """Cleanup when object is destroyed."""
        self.stop_spark_session()


def main() -> None:
    """Main function to run analytics."""
    logging.basicConfig(level=logging.INFO)
    
    analytics = SparkAnalytics()
    
    try:
        # Run full analysis
        results = analytics.run_full_analysis()
        
        # Print summary
        print("\nAnalytics Results Summary:")
        for name, df in results.items():
            print(f"\n{name}:")
            df.show(5)
        
        # Print data statistics
        stats = analytics.get_summary_statistics()
        print(f"\nData Statistics: {stats}")
        
    except Exception as e:
        logging.error(f"Error in analytics: {str(e)}")
        raise
    finally:
        analytics.stop_spark_session()


if __name__ == "__main__":
    main()
