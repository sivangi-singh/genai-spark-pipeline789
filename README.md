# E-Commerce Data Pipeline

A comprehensive Python-based data pipeline for generating and analyzing e-commerce data using PySpark.

## Project Purpose

This project demonstrates a complete e-commerce data pipeline that:
- Generates realistic fake customer, product, and order data for testing
- Analyzes the data using Apache Spark to derive business insights
- Provides a scalable framework for big data processing

## Project Structure

```
genai-spark-pipeline789/
├── src/                    # All Python code files
│   ├── __init__.py         # Package initialization
│   ├── config.py           # Configuration settings
│   ├── data_generator.py   # Fake data generation
│   └── spark_analytics.py  # PySpark analysis
├── data/
│   ├── raw/                # Generated raw data files
│   └── processed/          # Analyzed results
├── tests/                  # Test scripts
├── notebooks/              # Jupyter notebooks
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore file
└── README.md              # This file
```

## Features

### Data Generation
- **Customer Data**: Generate realistic customer profiles with demographics
- **Product Data**: Create product catalogs with categories and pricing
- **Order Data**: Simulate order transactions with timestamps and quantities

### Analytics
- **Sales Analysis**: Revenue trends, top-selling products
- **Customer Insights**: Customer segmentation, purchase patterns
- **Product Performance**: Category analysis, inventory insights

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd genai-spark-pipeline789
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Ensure Apache Spark is installed and configured on your system.

## Usage

### Generate Sample Data
```python
from src.data_generator import DataGenerator

generator = DataGenerator()
generator.generate_customers(num_customers=1000)
generator.generate_products(num_products=500)
generator.generate_orders(num_orders=5000)
```

### Run Analytics
```python
from src.spark_analytics import SparkAnalytics

analytics = SparkAnalytics()
analytics.run_full_analysis()
```

## Configuration

Edit `src/config.py` to modify:
- Data generation parameters
- Spark configuration settings
- File paths and output formats

## Testing

Run the test suite:
```bash
pytest tests/
```

## Requirements

- Python 3.8+
- Apache Spark 3.4+
- Java 8+

## License

This project is licensed under the MIT License.
