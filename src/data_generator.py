"""
Synthetic Data Generator for E-Commerce Pipeline

This module generates realistic synthetic e-commerce data including customers, products, and orders.
It uses Faker for realistic data generation, NumPy for statistical distributions, and tqdm for progress tracking.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from tqdm import tqdm


class SyntheticDataGenerator:
    """
    A comprehensive class to generate synthetic e-commerce data for testing and development.
    
    This class provides methods to generate:
    - Customer data with realistic demographics
    - Product data with categories and ratings
    - Order data following Pareto distribution (80/20 rule)
    
    Attributes:
        fake: Faker instance for generating realistic data
        logger: Logger instance for tracking operations
        customer_ids: List of generated customer IDs
        product_ids: List of generated product IDs
    """
    
    def __init__(self, random_seed: Optional[int] = None) -> None:
        """
        Initialize the SyntheticDataGenerator.
        
        Args:
            random_seed: Optional seed for reproducible results
        """
        if random_seed:
            random.seed(random_seed)
            np.random.seed(random_seed)
            Faker.seed(random_seed)
        
        self.fake = Faker()
        self.logger = self._setup_logging()
        
        # Initialize ID lists
        self.customer_ids: List[int] = []
        self.product_ids: List[int] = []
        
        # Product categories
        self.product_categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
        
        self.logger.info("SyntheticDataGenerator initialized")
    
    def _setup_logging(self) -> logging.Logger:
        """
        Setup logging configuration.
        
        Returns:
            Configured logger instance
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('data_generation.log')
            ]
        )
        return logging.getLogger(__name__)
    
    def generate_customers(self, num_customers: int = 100_000) -> pd.DataFrame:
        """
        Generate synthetic customer data with realistic demographics.
        
        Args:
            num_customers: Number of customers to generate (default: 100,000)
            
        Returns:
            DataFrame containing customer data with columns:
            - customer_id: Unique customer identifier
            - name: Full name
            - email: Email address
            - age: Age (normal distribution around 35)
            - city: City name
            - country: Country name
            - registration_date: Registration date
        """
        self.logger.info(f"Generating {num_customers:,} customers")
        
        customers = []
        
        for i in tqdm(range(num_customers), desc="Generating customers", unit="customers"):
            # Generate age with normal distribution around 35 (std=12, min=18, max=80)
            age = np.random.normal(35, 12)
            age = max(18, min(80, int(age)))
            
            customer = {
                'customer_id': i + 1,
                'name': self.fake.name(),
                'email': self.fake.email(),
                'age': age,
                'city': self.fake.city(),
                'country': self.fake.country(),
                'registration_date': self.fake.date_between(start_date='-5y', end_date='today')
            }
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        self.customer_ids = df['customer_id'].tolist()
        
        self.logger.info(f"Generated {len(df):,} customers successfully")
        return df
    
    def generate_products(self, num_products: int = 10_000) -> pd.DataFrame:
        """
        Generate synthetic product data with categories and ratings.
        
        Args:
            num_products: Number of products to generate (default: 10,000)
            
        Returns:
            DataFrame containing product data with columns:
            - product_id: Unique product identifier
            - name: Product name
            - category: Product category
            - price: Price (10-500)
            - stock: Stock quantity
            - rating: Rating (1-5)
        """
        self.logger.info(f"Generating {num_products:,} products")
        
        products = []
        
        for i in tqdm(range(num_products), desc="Generating products", unit="products"):
            category = random.choice(self.product_categories)
            
            # Generate price based on category
            if category == "Electronics":
                price = round(random.uniform(50, 500), 2)
            elif category == "Clothing":
                price = round(random.uniform(10, 200), 2)
            elif category == "Home":
                price = round(random.uniform(20, 300), 2)
            elif category == "Sports":
                price = round(random.uniform(15, 250), 2)
            else:  # Books
                price = round(random.uniform(10, 100), 2)
            
            # Generate rating with slight bias toward higher ratings
            rating = np.random.normal(4.0, 0.8)
            rating = max(1.0, min(5.0, round(rating, 1)))
            
            product = {
                'product_id': i + 1,
                'name': f"{category} {self.fake.catch_phrase()}",
                'category': category,
                'price': price,
                'stock': random.randint(10, 1000),
                'rating': rating
            }
            products.append(product)
        
        df = pd.DataFrame(products)
        self.product_ids = df['product_id'].tolist()
        
        self.logger.info(f"Generated {len(df):,} products successfully")
        return df
    
    def generate_orders(self, num_orders: int = 1_000_000) -> pd.DataFrame:
        """
        Generate synthetic order data following Pareto distribution (80/20 rule).
        
        Args:
            num_orders: Number of orders to generate (default: 1,000,000)
            
        Returns:
            DataFrame containing order data with columns:
            - order_id: Unique order identifier
            - customer_id: Customer identifier
            - product_id: Product identifier
            - quantity: Order quantity (1-10)
            - order_date: Order date
        """
        self.logger.info(f"Generating {num_orders:,} orders")
        
        # Ensure customers and products exist
        if not self.customer_ids:
            raise ValueError("Customer IDs not available. Generate customers first.")
        if not self.product_ids:
            raise ValueError("Product IDs not available. Generate products first.")
        
        orders = []
        
        # Generate customer order counts using Pareto distribution
        customer_order_counts = self._generate_pareto_distribution(
            len(self.customer_ids), num_orders, shape=1.16
        )
        
        order_id = 0
        
        for customer_idx, order_count in enumerate(tqdm(customer_order_counts, desc="Generating orders", unit="customers")):
            if order_count == 0:
                continue
                
            customer_id = self.customer_ids[customer_idx]
            
            for _ in range(int(order_count)):
                order_id += 1
                
                order = {
                    'order_id': order_id,
                    'customer_id': customer_id,
                    'product_id': random.choice(self.product_ids),
                    'quantity': random.randint(1, 10),
                    'order_date': self.fake.date_time_between(start_date='-1y', end_date='now')
                }
                orders.append(order)
        
        df = pd.DataFrame(orders)
        
        # Shuffle orders to randomize the order
        df = df.sample(frac=1).reset_index(drop=True)
        
        self.logger.info(f"Generated {len(df):,} orders successfully")
        return df
    
    def _generate_pareto_distribution(self, num_entities: int, total_orders: int, shape: float = 1.16) -> np.ndarray:
        """
        Generate Pareto distribution for order counts (80/20 rule).
        
        Args:
            num_entities: Number of entities (customers)
            total_orders: Total number of orders to distribute
            shape: Pareto distribution shape parameter (default: 1.16 for 80/20)
            
        Returns:
            Array of order counts for each entity
        """
        # Generate Pareto-distributed values
        pareto_values = np.random.pareto(shape, num_entities)
        
        # Normalize to get proportions
        proportions = pareto_values / pareto_values.sum()
        
        # Convert to order counts
        order_counts = proportions * total_orders
        
        # Round to integers and ensure sum equals total_orders
        order_counts = np.round(order_counts).astype(int)
        difference = total_orders - order_counts.sum()
        
        # Adjust for rounding error
        if difference != 0:
            # Add or subtract from the entity with highest count
            max_idx = np.argmax(order_counts)
            order_counts[max_idx] += difference
        
        return order_counts
    
    def generate_all_data(self, 
                         num_customers: int = 100_000,
                         num_products: int = 10_000,
                         num_orders: int = 1_000_000,
                         save_to_files: bool = True,
                         output_dir: str = "data/raw") -> Dict[str, pd.DataFrame]:
        """
        Generate complete synthetic e-commerce dataset.
        
        Args:
            num_customers: Number of customers to generate
            num_products: Number of products to generate
            num_orders: Number of orders to generate
            save_to_files: Whether to save data to CSV files
            output_dir: Directory to save files
            
        Returns:
            Dictionary containing all generated DataFrames
        """
        self.logger.info("Starting complete data generation")
        
        # Generate all data
        customers_df = self.generate_customers(num_customers)
        products_df = self.generate_products(num_products)
        orders_df = self.generate_orders(num_orders)
        
        # Save to files if requested
        if save_to_files:
            self._save_data_to_files({
                'customers': customers_df,
                'products': products_df,
                'orders': orders_df
            }, output_dir)
        
        self.logger.info("Complete data generation finished")
        
        return {
            'customers': customers_df,
            'products': products_df,
            'orders': orders_df
        }
    
    def _save_data_to_files(self, data_dict: Dict[str, pd.DataFrame], output_dir: str) -> None:
        """
        Save DataFrames to CSV files.
        
        Args:
            data_dict: Dictionary of DataFrames to save
            output_dir: Directory to save files
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for name, df in data_dict.items():
            file_path = output_path / f"{name}.csv"
            df.to_csv(file_path, index=False)
            self.logger.info(f"Saved {name} data to {file_path}")
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of generated data.
        
        Returns:
            Dictionary containing data summary
        """
        return {
            'total_customers': len(self.customer_ids),
            'total_products': len(self.product_ids),
            'product_categories': self.product_categories,
            'generation_timestamp': datetime.now().isoformat()
        }


def main() -> None:
    """
    Main function to demonstrate data generation.
    """
    logging.basicConfig(level=logging.INFO)
    
    # Initialize generator
    generator = SyntheticDataGenerator(random_seed=42)
    
    # Generate sample data (smaller for demo)
    print("Generating sample synthetic e-commerce data...")
    
    data = generator.generate_all_data(
        num_customers=1_000,  # Reduced for demo
        num_products=100,      # Reduced for demo
        num_orders=5_000,      # Reduced for demo
        save_to_files=True
    )
    
    # Display summary
    summary = generator.get_data_summary()
    print("\nData Generation Summary:")
    for key, value in summary.items():
        print(f"{key}: {value}")
    
    # Display sample data
    print("\nSample Data:")
    for name, df in data.items():
        print(f"\n{name} (shape: {df.shape}):")
        print(df.head(3))


if __name__ == "__main__":
    main()
