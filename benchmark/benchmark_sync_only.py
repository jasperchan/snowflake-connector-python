#!/usr/bin/env python3
"""
Benchmark script for Snowflake sync connector performance.

This script:
1. Sets up a test table with 100,000 rows 
2. Runs parallel queries using sync connector
3. Measures performance metrics
"""

import concurrent.futures
import os
import random
import statistics
import sys
import time
from typing import List, Tuple

# Add the local src directory to Python path to use development version
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

import snowflake.connector
from dotenv import load_dotenv
from convert_key import convert_pem_to_raw


class SnowflakeSyncBenchmark:
    """Benchmark for Snowflake sync connector performance."""
    
    def __init__(self, env_file: str = ".env"):
        """Initialize benchmark with credentials from env file."""
        load_dotenv(env_file)
        
        # Base connection parameters
        self.conn_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        }
        
        # Authentication - prefer private key over password
        private_key_pem = os.getenv('SNOWFLAKE_PRIVATE_KEY')
        private_key_raw = os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW')
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEYPHRASE')
        
        if private_key_pem:
            # Convert PEM to raw format that connector expects
            raw_key = convert_pem_to_raw(private_key_pem, passphrase)
            self.conn_params['private_key'] = raw_key
        elif private_key_raw:
            # Use private key from environment variable
            self.conn_params['private_key'] = private_key_raw
        elif private_key_path:
            # Use private key from file
            self.conn_params['private_key_file'] = private_key_path
        elif password:
            # Fall back to password authentication
            self.conn_params['password'] = password
        else:
            raise ValueError("Must provide either SNOWFLAKE_PRIVATE_KEY, SNOWFLAKE_PRIVATE_KEY_RAW, SNOWFLAKE_PRIVATE_KEY_PATH, or SNOWFLAKE_PASSWORD")
        
        # Validate required parameters
        required = ['user', 'account', 'database', 'schema', 'warehouse']
        missing = [k for k in required if not self.conn_params.get(k)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
            
        self.table_name = "BENCHMARK_TEST_TABLE_SYNC"
        self.row_count = 100000
        
    def setup_test_data(self) -> str:
        """Create and populate test table with data in a single query.
            
        Returns:
            The full table name that was created
        """
        table_name = self.table_name
        print(f"🔨 Setting up test table '{table_name}' with {self.row_count:,} rows...")
        print(f"   Database: {self.conn_params['database']}")
        print(f"   Schema: {self.conn_params['schema']}")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            
            # Use specified database and schema (must exist)
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create hybrid table with data in a single query for efficiency
            print("📊 Creating hybrid table with random data...")
            cursor.execute(f"""
                CREATE HYBRID TABLE {table_name} (
                    ID INTEGER PRIMARY KEY,
                    VAL STRING
                ) AS
                SELECT
                    SEQ4() as ID,
                    RANDSTR(50, RANDOM()) as VAL
                FROM TABLE(GENERATOR(ROWCOUNT => {self.row_count}))
            """)
            
            # Verify row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            actual_rows = cursor.fetchone()[0]
            print(f"✅ Test table '{table_name}' created with {actual_rows:,} rows")
            
            return table_name
            
        finally:
            conn.close()
    
    def cleanup_test_data(self, table_name: str) -> None:
        """Clean up test table.
        
        Args:
            table_name: Name of the table to drop
        """
        print(f"🧹 Cleaning up test table '{table_name}'...")
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"✅ Test table '{table_name}' cleaned up")
        finally:
            conn.close()
    
    def sync_query_worker(self, conn, table_name: str, query_id: int) -> Tuple[int, float, int]:
        """Execute a single sync query using provided connection."""
        start_time = time.time()
        
        cursor = conn.cursor()
        
        # Select random ID
        random_id = random.randint(0, self.row_count - 1)
        cursor.execute(f"SELECT ID, VAL FROM {table_name} WHERE ID = %s", (random_id,))
        
        result = cursor.fetchone()
        rows_fetched = 1 if result else 0
        cursor.close()
            
        elapsed = time.time() - start_time
        return query_id, elapsed, rows_fetched
    
    def benchmark_sync(self, num_queries: int, max_workers: int) -> Tuple[List[float], float]:
        """Benchmark sync connector with setup/teardown per benchmark."""
        # Create table for sync benchmark
        table_name = self.setup_test_data()
        
        try:
            print(f"🔄 Running {num_queries} sync queries with {max_workers} threads...")
            
            # Create one connection per worker thread
            connections = {}
            
            def worker_with_connection(query_id: int):
                import threading
                thread_id = threading.get_ident()
                
                # Create connection for this thread if not exists
                if thread_id not in connections:
                    conn = snowflake.connector.connect(**self.conn_params)
                    cursor = conn.cursor()
                    cursor.execute(f"USE DATABASE {self.conn_params['database']}")
                    cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
                    cursor.close()
                    connections[thread_id] = conn
                
                return self.sync_query_worker(connections[thread_id], table_name, query_id)
            
            start_time = time.time()
            query_times = []
            
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(worker_with_connection, i) 
                        for i in range(num_queries)
                    ]
                    
                    for future in concurrent.futures.as_completed(futures):
                        query_id, query_time, rows = future.result()
                        query_times.append(query_time)
            finally:
                # Close all connections
                for conn in connections.values():
                    conn.close()
            
            total_time = time.time() - start_time
            print(f"  Total time: {total_time:.2f}s")
            print(f"  Average query time: {statistics.mean(query_times):.3f}s")
            print(f"  Queries per second: {num_queries / total_time:.1f}")
            
            return query_times, total_time
            
        finally:
            # Clean up sync table
            self.cleanup_test_data(table_name)
    
    def print_performance_stats(self, sync_times: List[float], sync_total: float):
        """Print detailed performance statistics."""
        print("\n" + "="*60)
        print("📊 PERFORMANCE STATISTICS")
        print("="*60)
        
        sync_stats = {
            'mean': statistics.mean(sync_times),
            'median': statistics.median(sync_times),
            'min': min(sync_times),
            'max': max(sync_times),
            'stdev': statistics.stdev(sync_times) if len(sync_times) > 1 else 0
        }
        
        print(f"{'Metric':<15} {'Value':<12}")
        print("-" * 30)
        
        for metric in ['mean', 'median', 'min', 'max', 'stdev']:
            sync_val = sync_stats[metric]
            print(f"{metric.capitalize():<15} {sync_val:<12.3f}")
        
        print("\n📈 PERFORMANCE ANALYSIS:")
        
        # Throughput analysis
        sync_throughput = len(sync_times) / sync_total
        
        print(f"🚀 THROUGHPUT:")
        print(f"  • Queries per second: {sync_throughput:.1f}")
        print(f"  • Total queries: {len(sync_times)}")
        print(f"  • Total time: {sync_total:.2f}s")
        
        # Query latency analysis  
        sync_latency = statistics.mean(sync_times)
        
        print(f"\n⏱️  QUERY LATENCY:")
        print(f"  • Average: {sync_latency:.3f}s per query")
        print(f"  • Min: {min(sync_times):.3f}s")
        print(f"  • Max: {max(sync_times):.3f}s")

    def run_benchmark(
        self, 
        num_queries: int = 100,
        max_workers: int = 10
    ):
        """Run the complete benchmark."""
        print("🚀 Starting Snowflake Sync Connector Benchmark")
        print(f"   Queries: {num_queries}")
        print(f"   Max workers: {max_workers}")
        print(f"   Base table: {self.table_name} ({self.row_count:,} rows)")
        print()
        
        # Run sync benchmark (creates, uses, and cleans up its own table)
        sync_times, sync_total = self.benchmark_sync(num_queries, max_workers)
        
        # Print performance statistics
        self.print_performance_stats(sync_times, sync_total)


def main():
    """Main benchmark entry point."""
    # Parse command line arguments or use defaults
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark Snowflake sync connector")
    parser.add_argument("--queries", type=int, default=100, help="Number of queries to run (default: 100)")
    parser.add_argument("--workers", type=int, default=10, help="Max workers (default: 10)")
    parser.add_argument("--env-file", default=".env", help="Environment file path (default: .env)")
    
    args = parser.parse_args()
    
    try:
        benchmark = SnowflakeSyncBenchmark(args.env_file)
        benchmark.run_benchmark(
            num_queries=args.queries,
            max_workers=args.workers
        )
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()