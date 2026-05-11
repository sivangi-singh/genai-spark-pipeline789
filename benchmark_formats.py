#!/usr/bin/env python3
"""
File Format Benchmark with Hardware Metrics

This script benchmarks different file formats (CSV, XLSX, Parquet, ORC, Feather)
by measuring file size, read/write times, memory usage, CPU time, and energy consumption.
"""

import time
import tracemalloc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple
import logging

# Try to import optional dependencies
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("Warning: PyArrow not installed. Install with: pip install pyarrow")

try:
    import fastparquet
    FASTPARQUET_AVAILABLE = True
except ImportError:
    FASTPARQUET_AVAILABLE = False
    print("Warning: FastParquet not installed. Install with: pip install fastparquet")

try:
    import pyarrow.orc as orc
    ORC_AVAILABLE = True
except ImportError:
    ORC_AVAILABLE = False
    print("Warning: PyArrow ORC not available. Install with: pip install pyarrow")

try:
    import pyarrow.feather as feather
    FEATHER_AVAILABLE = True
except ImportError:
    FEATHER_AVAILABLE = False
    print("Warning: PyArrow Feather not available. Install with: pip install pyarrow")


class FileFormatBenchmark:
    """
    Benchmark class for comparing different file formats with hardware metrics.
    """
    
    def __init__(self, num_rows: int = 500_000):
        """
        Initialize the benchmark.
        
        Args:
            num_rows: Number of rows to generate for testing
        """
        self.num_rows = num_rows
        self.logger = self._setup_logging()
        self.results: Dict[str, Dict[str, Any]] = {}
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def generate_test_data(self) -> pd.DataFrame:
        """
        Generate test DataFrame with specified columns.
        
        Returns:
            DataFrame with test data
        """
        self.logger.info(f"Generating {self.num_rows:,} rows of test data...")
        
        # Categories for the category column
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Food', 'Beauty']
        
        data = {
            'id': range(1, self.num_rows + 1),
            'name': [f'Product_{i}' for i in range(1, self.num_rows + 1)],
            'email': [f'user{i}@example.com' for i in range(1, self.num_rows + 1)],
            'amount': np.random.uniform(1.0, 1000.0, self.num_rows).round(2),
            'date': pd.date_range('2020-01-01', periods=self.num_rows, freq='h'),
            'category': np.random.choice(categories, self.num_rows)
        }
        
        df = pd.DataFrame(data)
        self.logger.info(f"Generated DataFrame with shape: {df.shape}")
        return df
    
    def measure_operation(self, operation_func, *args, **kwargs) -> Tuple[float, float, float]:
        """
        Measure an operation's time, CPU time, and peak memory usage.
        
        Args:
            operation_func: Function to measure
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            Tuple of (wall_time, cpu_time, peak_memory_mb)
        """
        # Start memory tracking
        tracemalloc.start()
        
        # Measure wall time
        start_time = time.time()
        
        # Measure CPU time
        start_cpu_time = time.process_time()
        
        # Execute operation
        result = operation_func(*args, **kwargs)
        
        # Measure end times
        end_time = time.time()
        end_cpu_time = time.process_time()
        
        # Get peak memory usage
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        wall_time = end_time - start_time
        cpu_time = end_cpu_time - start_cpu_time
        peak_memory_mb = peak / (1024 * 1024)  # Convert to MB
        
        return wall_time, cpu_time, peak_memory_mb, result
    
    def benchmark_csv(self, df: pd.DataFrame, file_path: Path) -> Dict[str, Any]:
        """Benchmark CSV format."""
        self.logger.info("Benchmarking CSV format...")
        
        # Write operation
        write_time, write_cpu_time, write_memory, _ = self.measure_operation(
            df.to_csv, file_path, index=False
        )
        
        # Read operation
        read_time, read_cpu_time, read_memory, _ = self.measure_operation(
            pd.read_csv, file_path
        )
        
        # Get file size
        file_size = file_path.stat().st_size / (1024 * 1024)  # Convert to MB
        
        return {
            'format': 'CSV',
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'write_cpu_time_s': write_cpu_time,
            'read_cpu_time_s': read_cpu_time,
            'write_peak_memory_mb': write_memory,
            'read_peak_memory_mb': read_memory,
            'total_cpu_time_s': write_cpu_time + read_cpu_time,
            'total_time_s': write_time + read_time
        }
    
    def benchmark_excel(self, df: pd.DataFrame, file_path: Path) -> Dict[str, Any]:
        """Benchmark Excel (XLSX) format."""
        self.logger.info("Benchmarking Excel format...")
        
        # Write operation
        write_time, write_cpu_time, write_memory, _ = self.measure_operation(
            df.to_excel, file_path, index=False, engine='openpyxl'
        )
        
        # Read operation
        read_time, read_cpu_time, read_memory, _ = self.measure_operation(
            pd.read_excel, file_path, engine='openpyxl'
        )
        
        # Get file size
        file_size = file_path.stat().st_size / (1024 * 1024)  # Convert to MB
        
        return {
            'format': 'Excel',
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'write_cpu_time_s': write_cpu_time,
            'read_cpu_time_s': read_cpu_time,
            'write_peak_memory_mb': write_memory,
            'read_peak_memory_mb': read_memory,
            'total_cpu_time_s': write_cpu_time + read_cpu_time,
            'total_time_s': write_time + read_time
        }
    
    def benchmark_parquet(self, df: pd.DataFrame, file_path: Path) -> Dict[str, Any]:
        """Benchmark Parquet format."""
        self.logger.info("Benchmarking Parquet format...")
        
        # Write operation
        write_time, write_cpu_time, write_memory, _ = self.measure_operation(
            df.to_parquet, file_path, index=False, engine='pyarrow'
        )
        
        # Read operation
        read_time, read_cpu_time, read_memory, _ = self.measure_operation(
            pd.read_parquet, file_path, engine='pyarrow'
        )
        
        # Get file size
        file_size = file_path.stat().st_size / (1024 * 1024)  # Convert to MB
        
        return {
            'format': 'Parquet',
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'write_cpu_time_s': write_cpu_time,
            'read_cpu_time_s': read_cpu_time,
            'write_peak_memory_mb': write_memory,
            'read_peak_memory_mb': read_memory,
            'total_cpu_time_s': write_cpu_time + read_cpu_time,
            'total_time_s': write_time + read_time
        }
    
    def benchmark_orc(self, df: pd.DataFrame, file_path: Path) -> Dict[str, Any]:
        """Benchmark ORC format."""
        if not ORC_AVAILABLE:
            return {'format': 'ORC', 'error': 'PyArrow ORC not available'}
        
        self.logger.info("Benchmarking ORC format...")
        
        # Write operation
        write_time, write_cpu_time, write_memory, _ = self.measure_operation(
            lambda: pa.Table.from_pandas(df).write_to_file(file_path, format='orc')
        )
        
        # Read operation
        read_time, read_cpu_time, read_memory, _ = self.measure_operation(
            lambda: orc.read_table(file_path).to_pandas()
        )
        
        # Get file size
        file_size = file_path.stat().st_size / (1024 * 1024)  # Convert to MB
        
        return {
            'format': 'ORC',
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'write_cpu_time_s': write_cpu_time,
            'read_cpu_time_s': read_cpu_time,
            'write_peak_memory_mb': write_memory,
            'read_peak_memory_mb': read_memory,
            'total_cpu_time_s': write_cpu_time + read_cpu_time,
            'total_time_s': write_time + read_time
        }
    
    def benchmark_feather(self, df: pd.DataFrame, file_path: Path) -> Dict[str, Any]:
        """Benchmark Feather format."""
        if not FEATHER_AVAILABLE:
            return {'format': 'Feather', 'error': 'PyArrow Feather not available'}
        
        self.logger.info("Benchmarking Feather format...")
        
        # Write operation
        write_time, write_cpu_time, write_memory, _ = self.measure_operation(
            df.to_feather, file_path
        )
        
        # Read operation
        read_time, read_cpu_time, read_memory, _ = self.measure_operation(
            pd.read_feather, file_path
        )
        
        # Get file size
        file_size = file_path.stat().st_size / (1024 * 1024)  # Convert to MB
        
        return {
            'format': 'Feather',
            'file_size_mb': file_size,
            'write_time_s': write_time,
            'read_time_s': read_time,
            'write_cpu_time_s': write_cpu_time,
            'read_cpu_time_s': read_cpu_time,
            'write_peak_memory_mb': write_memory,
            'read_peak_memory_mb': read_memory,
            'total_cpu_time_s': write_cpu_time + read_cpu_time,
            'total_time_s': write_time + read_time
        }
    
    def calculate_energy_consumption(self, cpu_time_s: float, tdp_watts: float = 65) -> float:
        """
        Calculate estimated energy consumption.
        
        Args:
            cpu_time_s: CPU time in seconds
            tdp_watts: Thermal Design Power in watts (default: 65W)
            
        Returns:
            Energy consumption in Watt-hours (Wh)
        """
        return (cpu_time_s * tdp_watts) / 3600
    
    def run_benchmark(self) -> Dict[str, Dict[str, Any]]:
        """
        Run the complete benchmark for all formats.
        
        Returns:
            Dictionary with benchmark results
        """
        # Generate test data
        df = self.generate_test_data()
        
        # Create benchmark directory
        benchmark_dir = Path("benchmark_results")
        benchmark_dir.mkdir(exist_ok=True)
        
        # Run benchmarks for each format
        formats_to_test = [
            ('csv', self.benchmark_csv),
            ('xlsx', self.benchmark_excel),
            ('parquet', self.benchmark_parquet),
            ('orc', self.benchmark_orc),
            ('feather', self.benchmark_feather)
        ]
        
        for ext, benchmark_func in formats_to_test:
            file_path = benchmark_dir / f"test_data.{ext}"
            
            try:
                result = benchmark_func(df, file_path)
                
                if 'error' not in result:
                    # Calculate energy consumption
                    result['energy_consumption_wh'] = self.calculate_energy_consumption(
                        result['total_cpu_time_s']
                    )
                
                self.results[result['format']] = result
                
                # Clean up file
                if file_path.exists():
                    file_path.unlink()
                    
            except Exception as e:
                self.logger.error(f"Error benchmarking {ext}: {str(e)}")
                self.results[ext.upper()] = {'error': str(e)}
        
        return self.results
    
    def format_results_table(self) -> str:
        """
        Format benchmark results as a comparison table.
        
        Returns:
            Formatted table string
        """
        if not self.results:
            return "No benchmark results available."
        
        # Get CSV baseline for comparison
        csv_baseline = self.results.get('CSV', {})
        
        # Table header
        table = []
        header = [
            "Format", "Size (MB)", "Write (s)", "Read (s)", 
            "Total (s)", "CPU (s)", "Memory (MB)", "Energy (Wh)",
            "Size %", "Time %", "Energy %"
        ]
        table.append(header)
        
        # Add results for each format
        for format_name, result in self.results.items():
            if 'error' in result:
                row = [format_name, "ERROR", result['error'], "", "", "", "", "", "", "", ""]
                table.append(row)
                continue
            
            # Calculate percentages vs CSV baseline
            size_pct = (result['file_size_mb'] / csv_baseline.get('file_size_mb', 1)) * 100
            time_pct = (result['total_time_s'] / csv_baseline.get('total_time_s', 1)) * 100
            energy_pct = (result['energy_consumption_wh'] / csv_baseline.get('energy_consumption_wh', 1)) * 100
            
            row = [
                format_name,
                f"{result['file_size_mb']:.2f}",
                f"{result['write_time_s']:.3f}",
                f"{result['read_time_s']:.3f}",
                f"{result['total_time_s']:.3f}",
                f"{result['total_cpu_time_s']:.3f}",
                f"{result['write_peak_memory_mb'] + result['read_peak_memory_mb']:.2f}",
                f"{result['energy_consumption_wh']:.4f}",
                f"{size_pct:.1f}%",
                f"{time_pct:.1f}%",
                f"{energy_pct:.1f}%"
            ]
            table.append(row)
        
        # Format table
        col_widths = [max(len(str(row[i])) for row in table) for i in range(len(header))]
        
        formatted_table = []
        # Header
        header_row = " | ".join(f"{header[i]:<{col_widths[i]}}" for i in range(len(header)))
        formatted_table.append(header_row)
        formatted_table.append("-" * len(header_row))
        
        # Data rows
        for row in table[1:]:
            data_row = " | ".join(f"{str(row[i]):<{col_widths[i]}}" for i in range(len(row)))
            formatted_table.append(data_row)
        
        return "\n".join(formatted_table)
    
    def print_summary(self):
        """Print benchmark summary and recommendations."""
        print("\n" + "="*80)
        print("FILE FORMAT BENCHMARK RESULTS")
        print("="*80)
        
        print(f"\nTest Data: {self.num_rows:,} rows")
        print(f"CPU TDP: 65W (assumed)")
        print(f"Energy Calculation: CPU_time * TDP / 3600")
        
        print("\nComparison Table:")
        print(self.format_results_table())
        
        # Find best performers
        valid_results = {k: v for k, v in self.results.items() if 'error' not in v}
        
        if valid_results:
            print("\n" + "="*80)
            print("PERFORMANCE SUMMARY")
            print("="*80)
            
            # Best by file size
            best_size = min(valid_results.items(), key=lambda x: x[1]['file_size_mb'])
            print(f"\n📁 Smallest File Size: {best_size[0]} ({best_size[1]['file_size_mb']:.2f} MB)")
            
            # Best by write time
            best_write = min(valid_results.items(), key=lambda x: x[1]['write_time_s'])
            print(f"⚡ Fastest Write: {best_write[0]} ({best_write[1]['write_time_s']:.3f} s)")
            
            # Best by read time
            best_read = min(valid_results.items(), key=lambda x: x[1]['read_time_s'])
            print(f"📖 Fastest Read: {best_read[0]} ({best_read[1]['read_time_s']:.3f} s)")
            
            # Best by total time
            best_total = min(valid_results.items(), key=lambda x: x[1]['total_time_s'])
            print(f"🏆 Fastest Overall: {best_total[0]} ({best_total[1]['total_time_s']:.3f} s)")
            
            # Best by energy
            best_energy = min(valid_results.items(), key=lambda x: x[1]['energy_consumption_wh'])
            print(f"🌱 Most Energy Efficient: {best_energy[0]} ({best_energy[1]['energy_consumption_wh']:.4f} Wh)")
            
            # Best by memory
            best_memory = min(valid_results.items(), 
                          key=lambda x: x[1]['write_peak_memory_mb'] + x[1]['read_peak_memory_mb'])
            total_memory = best_memory[1]['write_peak_memory_mb'] + best_memory[1]['read_peak_memory_mb']
            print(f"💾 Lowest Memory Usage: {best_memory[0]} ({total_memory:.2f} MB)")


def main():
    """Main function to run the benchmark."""
    print("Starting File Format Benchmark...")
    print("This may take a few minutes depending on your system...\n")
    
    # Initialize and run benchmark
    benchmark = FileFormatBenchmark(num_rows=500_000)
    results = benchmark.run_benchmark()
    
    # Print results
    benchmark.print_summary()
    
    print(f"\nBenchmark completed! Check benchmark_results/ directory for any temporary files.")


if __name__ == "__main__":
    main()
