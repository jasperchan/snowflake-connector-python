#!/usr/bin/env python3
"""
Transaction Handling Benchmark - Sync version only.

This benchmark tests transaction handling performance including:
1. Basic transaction commits and rollbacks
2. Concurrent transaction handling
3. Transaction isolation levels
4. Large batch operations within transactions
"""

import concurrent.futures
import os
import sys
import time
import statistics
from typing import List, Tuple

# Add the local src directory to Python path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

import snowflake.connector
from dotenv import load_dotenv
from convert_key import convert_pem_to_raw


class TransactionSyncBenchmark:
    """Benchmark for testing transaction handling performance."""
    
    def __init__(self, env_file: str = ".env"):
        """Initialize benchmark with connection parameters."""
        load_dotenv(env_file)
        
        # Connection parameters
        self.conn_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        }
        
        # Add authentication
        private_key_pem = os.getenv('SNOWFLAKE_PRIVATE_KEY')
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        private_key_raw = os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEYPHRASE')
        
        if private_key_pem:
            # Convert PEM to raw format that connector expects
            raw_key = convert_pem_to_raw(private_key_pem, passphrase)
            self.conn_params['private_key'] = raw_key
        elif private_key_raw:
            self.conn_params['private_key'] = private_key_raw
        elif private_key_path:
            self.conn_params['private_key_file'] = private_key_path
        elif password:
            self.conn_params['password'] = password
        else:
            raise ValueError("Must provide authentication method")
            
        # Validate required parameters
        required = ['user', 'account', 'database', 'schema', 'warehouse']
        missing = [k for k in required if not self.conn_params.get(k)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
            
        self.table_name = "TRANSACTION_BENCHMARK_TABLE"
        
    def setup_test_table(self) -> None:
        """Create test table for transaction benchmarks."""
        print(f"🔨 Setting up test table '{self.table_name}'...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Drop and recreate table
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            cursor.execute(f"""
                CREATE TABLE {self.table_name} (
                    ID INTEGER,
                    VALUE VARCHAR(100),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            print("✅ Test table created")
        finally:
            conn.close()
            
    def cleanup_test_table(self) -> None:
        """Clean up test table."""
        print(f"🧹 Cleaning up test table '{self.table_name}'...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            print("✅ Test table cleaned up")
        finally:
            conn.close()
            
    def benchmark_basic_transactions(self, num_transactions: int = 100) -> Tuple[float, List[float]]:
        """Benchmark basic commit/rollback operations."""
        print(f"\n📊 Benchmarking {num_transactions} basic transactions...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        transaction_times = []
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            start_time = time.time()
            
            for i in range(num_transactions):
                trans_start = time.time()
                
                # Start transaction
                cursor.execute("BEGIN")
                
                # Insert some data
                cursor.execute(f"INSERT INTO {self.table_name} (ID, VALUE) VALUES (%s, %s)", 
                             (i, f"Transaction_{i}"))
                
                # Commit or rollback (alternate)
                if i % 2 == 0:
                    cursor.execute("COMMIT")
                else:
                    cursor.execute("ROLLBACK")
                    
                transaction_times.append(time.time() - trans_start)
                
            total_time = time.time() - start_time
            
            print(f"✅ Completed {num_transactions} transactions in {total_time:.2f}s")
            print(f"  Average transaction time: {statistics.mean(transaction_times):.3f}s")
            print(f"  Min/Max: {min(transaction_times):.3f}s / {max(transaction_times):.3f}s")
            
            return total_time, transaction_times
            
        finally:
            conn.close()
            
    def benchmark_batch_inserts(self, batch_size: int = 1000, num_batches: int = 10) -> Tuple[float, int]:
        """Benchmark batch insert operations within transactions."""
        print(f"\n📊 Benchmarking batch inserts ({num_batches} batches of {batch_size} rows)...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            start_time = time.time()
            total_rows = 0
            
            for batch_num in range(num_batches):
                # Start transaction
                cursor.execute("BEGIN")
                
                # Prepare batch data
                batch_data = [(batch_num * batch_size + i, f"Batch_{batch_num}_Row_{i}") 
                             for i in range(batch_size)]
                
                # Execute batch insert
                cursor.executemany(
                    f"INSERT INTO {self.table_name} (ID, VALUE) VALUES (%s, %s)",
                    batch_data
                )
                
                # Commit
                cursor.execute("COMMIT")
                total_rows += batch_size
                
                if (batch_num + 1) % 5 == 0:
                    print(f"  ... Completed {batch_num + 1} batches")
                    
            total_time = time.time() - start_time
            
            print(f"✅ Inserted {total_rows:,} rows in {total_time:.2f}s")
            print(f"  Rows per second: {total_rows / total_time:,.0f}")
            
            return total_time, total_rows
            
        finally:
            conn.close()
            
    def benchmark_concurrent_transactions(self, num_workers: int = 5, transactions_per_worker: int = 20) -> float:
        """Benchmark concurrent transaction handling."""
        print(f"\n📊 Benchmarking concurrent transactions ({num_workers} workers, {transactions_per_worker} each)...")
        
        def worker_task(worker_id: int) -> float:
            """Execute transactions for a single worker."""
            conn = snowflake.connector.connect(**self.conn_params)
            
            try:
                cursor = conn.cursor()
                cursor.execute(f"USE DATABASE {self.conn_params['database']}")
                cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
                
                worker_start = time.time()
                
                for i in range(transactions_per_worker):
                    cursor.execute("BEGIN")
                    cursor.execute(
                        f"INSERT INTO {self.table_name} (ID, VALUE) VALUES (%s, %s)",
                        (worker_id * 1000 + i, f"Worker_{worker_id}_Trans_{i}")
                    )
                    cursor.execute("COMMIT")
                    
                return time.time() - worker_start
                
            finally:
                conn.close()
                
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker_task, i) for i in range(num_workers)]
            worker_times = [f.result() for f in concurrent.futures.as_completed(futures)]
            
        total_time = time.time() - start_time
        total_transactions = num_workers * transactions_per_worker
        
        print(f"✅ Completed {total_transactions} concurrent transactions in {total_time:.2f}s")
        print(f"  Transactions per second: {total_transactions / total_time:.1f}")
        print(f"  Average worker time: {statistics.mean(worker_times):.2f}s")
        
        return total_time
        
    def run_benchmark(self):
        """Run the complete transaction benchmark suite."""
        print("🚀 Starting Transaction Handling Sync Benchmark")
        print(f"   Database: {self.conn_params['database']}")
        print(f"   Schema: {self.conn_params['schema']}")
        print()
        
        # Setup
        self.setup_test_table()
        
        try:
            # Run benchmarks
            results = {}
            
            # Basic transactions
            basic_total, basic_times = self.benchmark_basic_transactions(100)
            results['basic_transactions'] = {
                'total_time': basic_total,
                'avg_time': statistics.mean(basic_times),
                'count': len(basic_times)
            }
            
            # Batch inserts
            batch_time, batch_rows = self.benchmark_batch_inserts(1000, 10)
            results['batch_inserts'] = {
                'total_time': batch_time,
                'rows': batch_rows,
                'rows_per_sec': batch_rows / batch_time
            }
            
            # Concurrent transactions
            concurrent_time = self.benchmark_concurrent_transactions(5, 20)
            results['concurrent'] = {
                'total_time': concurrent_time,
                'transactions': 100,
                'tps': 100 / concurrent_time
            }
            
            # Print summary
            print("\n" + "="*60)
            print("📊 TRANSACTION BENCHMARK SUMMARY")
            print("="*60)
            
            print(f"\n{'Operation':<25} {'Time (s)':<12} {'Details':<30}")
            print("-" * 70)
            
            print(f"{'Basic Transactions':<25} {results['basic_transactions']['total_time']:<12.2f} "
                  f"{results['basic_transactions']['count']} transactions")
            print(f"{'Batch Inserts':<25} {results['batch_inserts']['total_time']:<12.2f} "
                  f"{results['batch_inserts']['rows']:,} rows ({results['batch_inserts']['rows_per_sec']:,.0f}/sec)")
            print(f"{'Concurrent Transactions':<25} {results['concurrent']['total_time']:<12.2f} "
                  f"{results['concurrent']['transactions']} trans ({results['concurrent']['tps']:.1f} TPS)")
                  
        finally:
            # Cleanup
            self.cleanup_test_table()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark transaction handling")
    parser.add_argument("--env-file", default=".env", help="Environment file path (default: .env)")
    
    args = parser.parse_args()
    
    try:
        benchmark = TransactionSyncBenchmark(args.env_file)
        benchmark.run_benchmark()
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()