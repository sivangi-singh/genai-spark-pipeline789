#!/usr/bin/env python3
"""
Optimized PySpark Session Configuration for 1M E-commerce Orders

This script provides an optimized SparkSession configuration for a laptop with:
- 16GB RAM
- 8 CPU cores
- Processing 1 million e-commerce orders
"""

import logging
from typing import Dict, Any

try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("PySpark not installed. Install with: pip install pyspark")


def create_optimized_spark_session(app_name: str = "ECommerceAnalytics") -> SparkSession:
    """
    Create optimized SparkSession for laptop with 16GB RAM and 8 CPU cores.
    
    Configuration explanations:
    1. spark.driver.memory: 8GB (50% of 16GB RAM for driver)
    2. spark.sql.shuffle.partitions: 8 (matches CPU cores for optimal parallelism)
    3. spark.sql.adaptive.enabled: true (auto-optimization for query plans)
    4. spark.serializer: KryoSerializer (faster binary serialization)
    5. spark.sql.adaptive.coalescePartitions.enabled: true (reduce partition overhead)
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        Configured SparkSession instance
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("PySpark is required but not installed")
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("Creating optimized SparkSession for 16GB RAM / 8 CPU cores laptop...")
    
    # Build Spark session with optimized configuration
    spark = (SparkSession.builder
        .appName(app_name)
        
        # === MEMORY CONFIGURATION ===
        # 1. spark.driver.memory: 8GB (50% of 16GB for driver)
        # Explanation: Allocates half of available RAM to Spark driver,
        # leaving 8GB for OS and other processes
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "2g")
        
        # Executor memory (for cluster mode, less relevant for local)
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "4")
        
        # === CPU CONFIGURATION ===
        # 2. spark.sql.shuffle.partitions: 8 (matches CPU cores)
        # Explanation: Sets number of partitions for shuffle operations to match
        # available CPU cores for optimal parallelism in groupBy operations
        .config("spark.sql.shuffle.partitions", "8")
        
        # === ADAPTIVE QUERY EXECUTION ===
        # 3. spark.sql.adaptive.enabled: true (auto-optimization)
        # Explanation: Enables Spark's cost-based optimizer to dynamically adjust
        # query plans based on runtime statistics
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        
        # === SERIALIZATION CONFIGURATION ===
        # 4. spark.serializer: KryoSerializer (faster data transfer)
        # Explanation: Uses Kryo serialization instead of Java serialization,
        # reducing network and disk I/O overhead by ~3-5x
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.kryoserializer.buffer.max", "512m")
        
        # === PERFORMANCE TUNING ===
        # 5. spark.sql.adaptive.coalescePartitions.enabled: true
        # Explanation: Automatically reduces number of partitions after shuffle
        # to avoid small partition overhead
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Additional optimizations for 1M rows dataset
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
        .config("spark.sql.broadcastTimeout", "36000")
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
        
        # === ARROW OPTIMIZATION ===
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        
        # === LOCAL MODE CONFIGURATION ===
        .master("local[*]")  # Use all available cores
        .config("spark.default.parallelism", "8")
        .config("spark.sql.shuffle.partitions", "8")
        
        .getOrCreate())
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    # Log configuration summary
    logger.info("SparkSession created with optimized configuration:")
    logger.info("  🧠 Memory Configuration:")
    logger.info(f"    - Driver Memory: 8GB (50% of 16GB)")
    logger.info(f"    - Executor Memory: 4GB")
    logger.info("  🔧 CPU Configuration:")
    logger.info(f"    - Shuffle Partitions: 8 (matches 8 CPU cores)")
    logger.info(f"    - Default Parallelism: 8")
    logger.info("  ⚡ Performance Optimizations:")
    logger.info("    - Adaptive Query Execution: ENABLED")
    logger.info("    - Kryo Serialization: ENABLED")
    logger.info("    - Adaptive Coalesce Partitions: ENABLED")
    logger.info("    - Arrow Optimization: ENABLED")
    logger.info("  📊 Dataset Optimizations:")
    logger.info("    - Broadcast Join Threshold: 10MB")
    logger.info("    - In-Memory Columnar Storage: ENABLED")
    
    return spark


def print_configuration_explanations():
    """Print detailed explanations of each configuration setting."""
    print("🚀 OPTIMIZED SPARK SESSION CONFIGURATION")
    print("=" * 60)
    print("\n📋 HARDWARE SPECIFICATIONS:")
    print("  - RAM: 16GB")
    print("  - CPU Cores: 8")
    print("  - Dataset: 1M e-commerce orders")
    
    print("\n⚙️  CONFIGURATION SETTINGS WITH EXPLANATIONS:")
    print("-" * 60)
    
    configs = [
        {
            "setting": "spark.driver.memory",
            "value": "8g",
            "explanation": "Allocates 8GB (50% of 16GB) to Spark driver\n" +
                       "Leaves 8GB for OS and other processes\n" +
                       "Optimal for 1M row dataset processing"
        },
        {
            "setting": "spark.sql.shuffle.partitions", 
            "value": "8",
            "explanation": "Sets shuffle partitions to match CPU cores\n" +
                       "Enables optimal parallelism for groupBy operations\n" +
                       "Reduces data skew in large aggregations"
        },
        {
            "setting": "spark.sql.adaptive.enabled",
            "value": "true", 
            "explanation": "Enables cost-based query optimizer\n" +
                       "Dynamically adjusts execution plans\n" +
                       "Improves performance based on data statistics"
        },
        {
            "setting": "spark.serializer",
            "value": "KryoSerializer",
            "explanation": "Uses Kryo instead of Java serialization\n" +
                       "3-5x faster data transfer\n" +
                       "Reduces network and disk I/O overhead"
        },
        {
            "setting": "spark.sql.adaptive.coalescePartitions.enabled",
            "value": "true",
            "explanation": "Automatically reduces partitions after shuffle\n" +
                       "Avoids small partition overhead\n" +
                       "Optimizes for final result size"
        }
    ]
    
    for i, config in enumerate(configs, 1):
        print(f"\n{i}. {config['setting']}: {config['value']}")
        print(f"   📝 Explanation: {config['explanation']}")
    
    print("\n💡 USAGE EXAMPLE:")
    print("-" * 60)
    print("```python")
    print("from optimized_spark_config import create_optimized_spark_session")
    print("")
    print("# Create optimized session")
    print("spark = create_optimized_spark_session('MyECommerceApp')")
    print("")
    print("# Use for 1M order processing")
    print("orders_df = spark.read.parquet('data/raw/orders.parquet')")
    print("result = orders_df.groupBy('customer_id').sum('amount').orderBy(desc('sum(amount)'))")
    print("result.show()")
    print("```")
    
    print("\n🎯 PERFORMANCE EXPECTATIONS:")
    print("-" * 60)
    print("  📈 For 1M rows on 16GB/8-core laptop:")
    print("     - Initial load: 10-20 seconds")
    print("     - GroupBy operations: 5-15 seconds") 
    print("     - Join operations: 8-25 seconds")
    print("     - Memory usage: ~2-4GB RAM")
    print("     - Disk I/O: Minimal with proper partitioning")


def main():
    """Main function to demonstrate configuration."""
    try:
        print_configuration_explanations()
        
        print("\n🔥 Testing optimized session creation...")
        spark = create_optimized_spark_session()
        
        print("✅ Optimized SparkSession created successfully!")
        print(f"   Session ID: {spark.sparkContext.applicationId}")
        print(f"   Version: {spark.version}")
        
        # Stop session
        spark.stop()
        print("✅ SparkSession stopped")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    main()
