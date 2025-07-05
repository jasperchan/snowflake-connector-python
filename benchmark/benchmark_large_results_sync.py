#!/usr/bin/env python3
"""
Large Result Set Benchmark - Testing sync connector for large data retrieval.

This benchmark:
1. Creates a table with 1M+ rows of test data
2. Performs queries with stable sorting
3. Measures performance for large result set fetching
4. Tests memory efficiency and streaming behavior
"""

import hashlib
import os
import statistics
import sys
import time
from typing import List, Tuple, Any

# Add the local src directory to Python path to use development version
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

from dotenv import load_dotenv
import snowflake.connector
from convert_key import convert_pem_to_raw


class LargeResultSyncBenchmark:
    """Benchmark for testing large result set retrieval performance."""
    
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
            
        self.table_name = "LARGE_RESULT_BENCHMARK_TABLE"
        self.row_count = 1000000  # 1M rows
        
    def setup_test_data(self) -> None:
        """Create and populate test table with 1M rows."""
        print(f"🔨 Setting up test table '{self.table_name}' with {self.row_count:,} rows...")
        print(f"   Database: {self.conn_params['database']}")
        print(f"   Schema: {self.conn_params['schema']}")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            
            # Use database and schema
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            
            # Create table with meaningful data
            print("📊 Creating table with test data...")
            cursor.execute(f"""
                CREATE TABLE {self.table_name} AS
                SELECT
                    SEQ4() as ID,
                    UNIFORM(1, 1000000, RANDOM()) as USER_ID,
                    RANDSTR(20, RANDOM()) as USERNAME,
                    UNIFORM(18, 80, RANDOM()) as AGE,
                    UNIFORM(1000, 150000, RANDOM()) as SALARY,
                    RANDSTR(100, RANDOM()) as DESCRIPTION,
                    DATEADD(day, -UNIFORM(0, 3650, RANDOM()), CURRENT_DATE()) as CREATED_DATE,
                    CASE UNIFORM(1, 5, RANDOM())
                        WHEN 1 THEN 'ACTIVE'
                        WHEN 2 THEN 'INACTIVE'
                        WHEN 3 THEN 'PENDING'
                        WHEN 4 THEN 'SUSPENDED'
                        ELSE 'DELETED'
                    END as STATUS
                FROM TABLE(GENERATOR(ROWCOUNT => {self.row_count}))
            """)
            
            # Verify row count
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            actual_rows = cursor.fetchone()[0]
            print(f"✅ Test table created with {actual_rows:,} rows")
            
        finally:
            conn.close()
            
    def cleanup_test_data(self) -> None:
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
            
    def benchmark_full_table_scan(self) -> Tuple[float, int, str]:
        """Benchmark fetching all rows from the table."""
        print("\n📊 Benchmarking full table scan...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Execute query
            start_time = time.time()
            cursor.execute(f"SELECT * FROM {self.table_name} ORDER BY ID")
            
            # Fetch all results and calculate checksum
            row_count = 0
            hash_obj = hashlib.sha256()
            
            print("  Fetching rows...")
            while True:
                # Fetch in batches for memory efficiency
                rows = cursor.fetchmany(10000)
                if not rows:
                    break
                    
                row_count += len(rows)
                if row_count % 100000 == 0:
                    print(f"  ... {row_count:,} rows fetched")
                    
                # Add rows to checksum
                for row in rows:
                    row_str = str(row)
                    hash_obj.update(row_str.encode())
                    
            total_time = time.time() - start_time
            checksum = hash_obj.hexdigest()
            
            print(f"✅ Fetched {row_count:,} rows in {total_time:.2f}s")
            print(f"  Rows per second: {row_count / total_time:,.0f}")
            print(f"  Checksum: {checksum[:16]}...")
            
            return total_time, row_count, checksum
            
        finally:
            conn.close()
            
    def benchmark_filtered_query(self, filter_percentage: float = 10.0) -> Tuple[float, int]:
        """Benchmark fetching a percentage of rows with filtering."""
        print(f"\n📊 Benchmarking filtered query ({filter_percentage}% of data)...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Calculate filter threshold
            threshold = int(1000000 * (filter_percentage / 100))
            
            # Execute filtered query
            start_time = time.time()
            cursor.execute(f"""
                SELECT * FROM {self.table_name} 
                WHERE USER_ID <= %s
                ORDER BY ID
            """, (threshold,))
            
            # Fetch all results
            row_count = 0
            print("  Fetching filtered rows...")
            while True:
                rows = cursor.fetchmany(10000)
                if not rows:
                    break
                row_count += len(rows)
                    
            total_time = time.time() - start_time
            
            print(f"✅ Fetched {row_count:,} rows in {total_time:.2f}s")
            print(f"  Rows per second: {row_count / total_time:,.0f}")
            
            return total_time, row_count
            
        finally:
            conn.close()
            
    def benchmark_aggregation_query(self) -> Tuple[float, int]:
        """Benchmark aggregation queries."""
        print("\n📊 Benchmarking aggregation query...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Execute aggregation query
            start_time = time.time()
            cursor.execute(f"""
                SELECT 
                    STATUS,
                    COUNT(*) as COUNT,
                    AVG(AGE) as AVG_AGE,
                    MIN(SALARY) as MIN_SALARY,
                    MAX(SALARY) as MAX_SALARY,
                    AVG(SALARY) as AVG_SALARY
                FROM {self.table_name}
                GROUP BY STATUS
                ORDER BY COUNT DESC
            """)
            
            # Fetch results
            results = cursor.fetchall()
            total_time = time.time() - start_time
            
            print(f"✅ Aggregation completed in {total_time:.2f}s")
            print("  Results by status:")
            for row in results:
                print(f"    {row[0]}: {row[1]:,} rows, avg age: {row[2]:.1f}, avg salary: ${row[5]:,.2f}")
                
            return total_time, len(results)
            
        finally:
            conn.close()
            
    def run_benchmark(self):
        """Run the complete benchmark suite."""
        print("🚀 Starting Large Result Set Sync Benchmark")
        print(f"   Table: {self.table_name}")
        print(f"   Target rows: {self.row_count:,}")
        print()
        
        # Setup test data
        self.setup_test_data()
        
        try:
            # Run benchmarks
            results = {}
            
            # Full table scan
            full_time, full_rows, full_checksum = self.benchmark_full_table_scan()
            results['full_scan'] = {
                'time': full_time,
                'rows': full_rows,
                'rows_per_sec': full_rows / full_time,
                'checksum': full_checksum[:16]
            }
            
            # Filtered queries
            for filter_pct in [1.0, 10.0, 50.0]:
                filter_time, filter_rows = self.benchmark_filtered_query(filter_pct)
                results[f'filter_{filter_pct}pct'] = {
                    'time': filter_time,
                    'rows': filter_rows,
                    'rows_per_sec': filter_rows / filter_time
                }
                
            # Aggregation query
            agg_time, agg_groups = self.benchmark_aggregation_query()
            results['aggregation'] = {
                'time': agg_time,
                'groups': agg_groups
            }
            
            # Print summary
            print("\n" + "="*60)
            print("📊 BENCHMARK SUMMARY")
            print("="*60)
            
            print(f"\n{'Operation':<20} {'Time (s)':<12} {'Rows':<15} {'Rows/sec':<15}")
            print("-" * 65)
            
            for op, metrics in results.items():
                if 'rows_per_sec' in metrics:
                    print(f"{op:<20} {metrics['time']:<12.2f} {metrics['rows']:<15,} {metrics['rows_per_sec']:<15,.0f}")
                elif op == 'aggregation':
                    print(f"{op:<20} {metrics['time']:<12.2f} {metrics['groups']} groups")
                    
        finally:
            # Cleanup
            self.cleanup_test_data()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark large result set retrieval")
    parser.add_argument("--env-file", default=".env", help="Environment file path (default: .env)")
    
    args = parser.parse_args()
    
    try:
        benchmark = LargeResultSyncBenchmark(args.env_file)
        benchmark.run_benchmark()
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()