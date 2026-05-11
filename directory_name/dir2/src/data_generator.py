"""
Data Generator Module for E-Commerce Pipeline

This module generates realistic fake e-commerce data including customers, products, and orders.
It uses the Faker library to create believable test data for analytics purposes.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from .config import config


class DataGenerator:
    """
    A class to generate fake e-commerce data for testing and development.
    
    This class provides methods to generate:
    - Customer data with demographics
    - Product data with categories and pricing
    - Order data with transactions
    """
    
    def __init__(self) -> None:
        """Initialize the DataGenerator with Faker and configuration."""
        self.fake = Faker()
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize customer and product IDs
        self.customer_ids: List[int] = []
        self.product_ids: List[int] = []
        
        self.logger.info("DataGenerator initialized")
    
    def generate_customers(self, num_customers: Optional[int] = None) -> pd.DataFrame:
        """
        Generate fake customer data.
        
        Args:
            num_customers: Number of customers to generate. Defaults to config value.
            
        Returns:
            DataFrame containing customer data
        """
        if num_customers is None:
            num_customers = self.config.default_customers
        
        self.logger.info(f"Generating {num_customers} customers")
        
        customers = []
        for i in range(1, num_customers + 1):
            # Generate age based on age ranges
            age_group = random.choice(list(self.config.age_ranges.keys()))
            age_range = self.config.age_ranges[age_group]
            age = random.randint(age_range[0], age_range[1])
            
            customer = {
                'customer_id': i,
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'age': age,
                'age_group': age_group,
                'gender': random.choice(['Male', 'Female', 'Other']),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'country': self.fake.country(),
                'registration_date': self.fake.date_between(start_date='-5y', end_date='today'),
                'last_login': self.fake.date_time_between(start_date='-30d', end_date='now'),
                'is_active': random.choice([True, True, True, False])  # 75% active
            }
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        self.customer_ids = df['customer_id'].tolist()
        
        # Save to file
        output_path = self.config.get_file_path('raw', 'customers.csv')
        df.to_csv(output_path, index=False)
        self.logger.info(f"Customer data saved to {output_path}")
        
        return df
    
    def generate_products(self, num_products: Optional[int] = None) -> pd.DataFrame:
        """
        Generate fake product data.
        
        Args:
            num_products: Number of products to generate. Defaults to config value.
            
        Returns:
            DataFrame containing product data
        """
        if num_products is None:
            num_products = self.config.default_products
        
        self.logger.info(f"Generating {num_products} products")
        
        products = []
        for i in range(1, num_products + 1):
            category = random.choice(self.config.product_categories)
            price_range = self.config.price_ranges[category]
            
            product = {
                'product_id': i,
                'product_name': f"{category} {self.fake.catch_phrase()}",
                'category': category,
                'description': self.fake.text(max_nb_chars=200),
                'price': round(random.uniform(price_range[0], price_range[1]), 2),
                'cost': round(random.uniform(price_range[0] * 0.3, price_range[1] * 0.6), 2),
                'stock_quantity': random.randint(10, 1000),
                'weight': round(random.uniform(0.1, 50.0), 2),
                'brand': self.fake.company(),
                'sku': f"SKU-{i:06d}",
                'created_date': self.fake.date_between(start_date='-2y', end_date='today'),
                'is_active': random.choice([True, True, True, False])  # 75% active
            }
            products.append(product)
        
        df = pd.DataFrame(products)
        self.product_ids = df['product_id'].tolist()
        
        # Save to file
        output_path = self.config.get_file_path('raw', 'products.csv')
        df.to_csv(output_path, index=False)
        self.logger.info(f"Product data saved to {output_path}")
        
        return df
    
    def generate_orders(self, num_orders: Optional[int] = None) -> pd.DataFrame:
        """
        Generate fake order data.
        
        Args:
            num_orders: Number of orders to generate. Defaults to config value.
            
        Returns:
            DataFrame containing order data
        """
        if num_orders is None:
            num_orders = self.config.default_orders
        
        # Ensure customers and products exist
        if not self.customer_ids:
            self.generate_customers()
        if not self.product_ids:
            self.generate_products()
        
        self.logger.info(f"Generating {num_orders} orders")
        
        orders = []
        order_items = []
        
        for i in range(1, num_orders + 1):
            customer_id = random.choice(self.customer_ids)
            order_date = self.fake.date_time_between(start_date='-1y', end_date='now')
            
            # Generate order items (1-5 items per order)
            num_items = random.randint(1, 5)
            order_total = 0.0
            
            for item_num in range(num_items):
                product_id = random.choice(self.product_ids)
                quantity = random.randint(1, 5)
                
                # Get product price (in real scenario, this would come from product data)
                item_price = round(random.uniform(10.0, 500.0), 2)
                item_total = item_price * quantity
                order_total += item_total
                
                order_item = {
                    'order_item_id': len(order_items) + 1,
                    'order_id': i,
                    'product_id': product_id,
                    'quantity': quantity,
                    'unit_price': item_price,
                    'total_price': item_total
                }
                order_items.append(order_item)
            
            order = {
                'order_id': i,
                'customer_id': customer_id,
                'order_date': order_date,
                'order_status': random.choice(['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']),
                'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
                'shipping_address': self.fake.address().replace('\n', ', '),
                'order_total': round(order_total, 2),
                'shipping_cost': round(random.uniform(5.0, 25.0), 2),
                'tax_amount': round(order_total * 0.08, 2),
                'discount_amount': round(random.uniform(0.0, order_total * 0.1), 2),
                'final_amount': round(order_total + 25.0 + (order_total * 0.08) - random.uniform(0.0, order_total * 0.1), 2)
            }
            orders.append(order)
        
        orders_df = pd.DataFrame(orders)
        order_items_df = pd.DataFrame(order_items)
        
        # Save to files
        orders_path = self.config.get_file_path('raw', 'orders.csv')
        order_items_path = self.config.get_file_path('raw', 'order_items.csv')
        
        orders_df.to_csv(orders_path, index=False)
        order_items_df.to_csv(order_items_path, index=False)
        
        self.logger.info(f"Orders data saved to {orders_path}")
        self.logger.info(f"Order items data saved to {order_items_path}")
        
        return orders_df, order_items_df
    
    def generate_all_data(self, 
                         num_customers: Optional[int] = None,
                         num_products: Optional[int] = None,
                         num_orders: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """
        Generate all types of data (customers, products, orders).
        
        Args:
            num_customers: Number of customers to generate
            num_products: Number of products to generate
            num_orders: Number of orders to generate
            
        Returns:
            Dictionary containing all generated DataFrames
        """
        self.logger.info("Starting complete data generation")
        
        customers_df = self.generate_customers(num_customers)
        products_df = self.generate_products(num_products)
        orders_df, order_items_df = self.generate_orders(num_orders)
        
        result = {
            'customers': customers_df,
            'products': products_df,
            'orders': orders_df,
            'order_items': order_items_df
        }
        
        self.logger.info("Complete data generation finished")
        return result
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get a summary of generated data.
        
        Returns:
            Dictionary containing data summary statistics
        """
        summary = {
            'total_customers': len(self.customer_ids),
            'total_products': len(self.product_ids),
            'data_directory': str(self.config.raw_data_dir),
            'output_format': self.config.output_format
        }
        
        # Check if data files exist and get their sizes
        data_files = ['customers.csv', 'products.csv', 'orders.csv', 'order_items.csv']
        file_info = {}
        
        for file_name in data_files:
            file_path = self.config.raw_data_dir / file_name
            if file_path.exists():
                file_info[file_name] = {
                    'size_bytes': file_path.stat().st_size,
                    'last_modified': datetime.fromtimestamp(file_path.stat().st_mtime)
                }
        
        summary['files'] = file_info
        return summary


def main() -> None:
    """Main function to run data generation."""
    logging.basicConfig(level=logging.INFO)
    
    generator = DataGenerator()
    
    # Generate sample data
    data = generator.generate_all_data(
        num_customers=100,
        num_products=50,
        num_orders=500
    )
    
    # Print summary
    summary = generator.get_data_summary()
    print("\nData Generation Summary:")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
