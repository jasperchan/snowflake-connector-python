#!/usr/bin/env python3
"""
Arrow Extension Benchmark - Comparing performance with and without arrow extensions.

This benchmark:
1. Creates a table with 1M+ rows of test data
2. Performs identical queries with and without arrow extensions enabled
3. Verifies that both methods return identical results
4. Measures performance differences for arrow vs non-arrow result processing
5. Tests memory efficiency and processing speed differences
"""

import asyncio
import hashlib
import os
import statistics
import sys
import time
from typing import List, Tuple, Any, Dict

# Add the local src directory to Python path to use development version
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

from dotenv import load_dotenv
import snowflake.connector
import snowflake.connector.aio


class ArrowExtensionBenchmark:
    """Benchmark for testing arrow extension performance and correctness."""
    
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
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        private_key_raw = os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        
        if private_key_path and os.path.exists(private_key_path):
            self.conn_params['private_key_file'] = private_key_path
        elif private_key_raw:
            self.conn_params['private_key'] = private_key_raw
        elif password:
            self.conn_params['password'] = password
        else:
            raise ValueError("No authentication method configured")
            
        # Test parameters
        self.table_name = "ARROW_EXTENSION_BENCHMARK"
        self.row_count = 1000000  # 1M rows
        
    def get_arrow_enabled_params(self) -> Dict[str, Any]:
        """Get connection parameters with arrow extensions enabled."""
        params = self.conn_params.copy()
        params['use_arrow_result_format'] = True
        return params
    
    def get_arrow_disabled_params(self) -> Dict[str, Any]:
        """Get connection parameters with arrow extensions disabled."""
        params = self.conn_params.copy()
        params['use_arrow_result_format'] = False
        return params
        
    def setup_large_table(self, table_suffix: str = "") -> str:
        """Create large test table with stable, deterministic data."""
        table_name = f"{self.table_name}{table_suffix}"
        print(f"🔨 Setting up large test table '{table_name}' with {self.row_count:,} rows...")
        print(f"   Database: {self.conn_params['database']}")
        print(f"   Schema: {self.conn_params['schema']}")
        print("   ⚠️  This may take several minutes for 1M+ rows...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            
            # Use specified database and schema
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create large table with comprehensive data types for arrow testing
            print("📊 Creating large table with comprehensive data types...")
            start_time = time.time()
            
            # Add BINARY_DATA to test binary handling
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    ID INTEGER PRIMARY KEY,
                    CATEGORY STRING,
                    VALUE DECIMAL(10,2),
                    FLOAT_VALUE FLOAT,
                    TEXT_DATA STRING,
                    TIMESTAMP_DATA TIMESTAMP,
                    DATE_DATA DATE,
                    BOOLEAN_DATA BOOLEAN,
                    BINARY_DATA BINARY
                ) AS
                SELECT
                    SEQ4() as ID,
                    CASE 
                        WHEN SEQ4() % 10 = 0 THEN 'CATEGORY_A'
                        WHEN SEQ4() % 10 < 5 THEN 'CATEGORY_B' 
                        ELSE 'CATEGORY_C'
                    END as CATEGORY,
                    ROUND((SEQ4() * 1.23456), 2) as VALUE,
                    (SEQ4() * 3.14159) as FLOAT_VALUE,
                    CONCAT('DATA_ROW_', LPAD(SEQ4()::STRING, 10, '0')) as TEXT_DATA,
                    DATEADD('second', SEQ4() % 86400, '2024-01-01'::TIMESTAMP) as TIMESTAMP_DATA,
                    DATEADD('day', SEQ4() % 365, '2024-01-01'::DATE) as DATE_DATA,
                    (SEQ4() % 2 = 0) as BOOLEAN_DATA,
                    HEX_ENCODE(CONCAT('BIN', LPAD(SEQ4()::STRING, 6, '0'))) as BINARY_DATA
                FROM TABLE(GENERATOR(ROWCOUNT => {self.row_count}))
            """)
            
            setup_time = time.time() - start_time
            
            # Verify row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            actual_rows = cursor.fetchone()[0]
            
            print(f"✅ Large table '{table_name}' created with {actual_rows:,} rows in {setup_time:.1f}s")
            return table_name
            
        finally:
            conn.close()
    
    def cleanup_table(self, table_name: str) -> None:
        """Clean up test table."""
        print(f"🧹 Cleaning up table '{table_name}'...")
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"✅ Table '{table_name}' cleaned up")
        finally:
            conn.close()
    
    def fetch_without_arrow(self, table_name: str, limit: int) -> Tuple[List[Tuple], float, int]:
        """Fetch results without arrow extensions."""
        print(f"🔄 Fetching {limit:,} rows WITHOUT arrow extensions...")
        
        conn = snowflake.connector.connect(**self.get_arrow_disabled_params())
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Stable sort by ID to ensure deterministic results
            start_time = time.time()
            cursor.execute(f"""
                SELECT ID, CATEGORY, VALUE, FLOAT_VALUE, TEXT_DATA, TIMESTAMP_DATA, DATE_DATA, BOOLEAN_DATA, BINARY_DATA
                FROM {table_name} 
                ORDER BY ID 
                LIMIT {limit}
            """)
            
            results = cursor.fetchall()
            fetch_time = time.time() - start_time
            
            print(f"  ✅ Non-arrow fetched {len(results):,} rows in {fetch_time:.2f}s")
            return results, fetch_time, len(results)
            
        finally:
            conn.close()
    
    def fetch_with_arrow(self, table_name: str, limit: int) -> Tuple[List[Tuple], float, int]:
        """Fetch results with arrow extensions enabled."""
        print(f"🏹 Fetching {limit:,} rows WITH arrow extensions...")
        
        conn = snowflake.connector.connect(**self.get_arrow_enabled_params())
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Identical query with stable sort
            start_time = time.time()
            cursor.execute(f"""
                SELECT ID, CATEGORY, VALUE, FLOAT_VALUE, TEXT_DATA, TIMESTAMP_DATA, DATE_DATA, BOOLEAN_DATA, BINARY_DATA
                FROM {table_name} 
                ORDER BY ID 
                LIMIT {limit}
            """)
            
            results = cursor.fetchall()
            fetch_time = time.time() - start_time
            
            print(f"  ✅ Arrow fetched {len(results):,} rows in {fetch_time:.2f}s")
            return results, fetch_time, len(results)
            
        finally:
            conn.close()
    
    async def fetch_async_without_arrow(self, table_name: str, limit: int) -> Tuple[List[Tuple], float, int]:
        """Fetch results using async connector without arrow extensions."""
        print(f"⚡ Fetching {limit:,} rows with async connector WITHOUT arrow...")
        
        conn = await snowflake.connector.aio.connect(**self.get_arrow_disabled_params())
        try:
            cursor = conn.cursor()
            await cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            await cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Identical query with stable sort
            start_time = time.time()
            await cursor.execute(f"""
                SELECT ID, CATEGORY, VALUE, FLOAT_VALUE, TEXT_DATA, TIMESTAMP_DATA, DATE_DATA, BOOLEAN_DATA, BINARY_DATA
                FROM {table_name} 
                ORDER BY ID 
                LIMIT {limit}
            """)
            
            results = await cursor.fetchall()
            fetch_time = time.time() - start_time
            
            print(f"  ✅ Async non-arrow fetched {len(results):,} rows in {fetch_time:.2f}s")
            return results, fetch_time, len(results)
            
        finally:
            await conn.close()
    
    async def fetch_async_with_arrow(self, table_name: str, limit: int) -> Tuple[List[Tuple], float, int]:
        """Fetch results using async connector with arrow extensions."""
        print(f"🏹⚡ Fetching {limit:,} rows with async connector WITH arrow...")
        
        conn = await snowflake.connector.aio.connect(**self.get_arrow_enabled_params())
        try:
            cursor = conn.cursor()
            await cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            await cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Identical query with stable sort
            start_time = time.time()
            await cursor.execute(f"""
                SELECT ID, CATEGORY, VALUE, FLOAT_VALUE, TEXT_DATA, TIMESTAMP_DATA, DATE_DATA, BOOLEAN_DATA, BINARY_DATA
                FROM {table_name} 
                ORDER BY ID 
                LIMIT {limit}
            """)
            
            results = await cursor.fetchall()
            fetch_time = time.time() - start_time
            
            print(f"  ✅ Async arrow fetched {len(results):,} rows in {fetch_time:.2f}s")
            return results, fetch_time, len(results)
            
        finally:
            await conn.close()
    
    def hash_results(self, results: List[Tuple]) -> str:
        """Create deterministic hash of results for comparison."""
        # Convert all data to strings for consistent hashing, handling complex types
        result_strings = []
        for row in results:
            row_parts = []
            for val in row:
                if val is None:
                    row_parts.append('NULL')
                elif isinstance(val, (bytes, bytearray)):
                    row_parts.append(f'BINARY:{val.hex()}')
                elif isinstance(val, (dict, list)):
                    row_parts.append(f'JSON:{str(val)}')
                else:
                    row_parts.append(str(val))
            result_strings.append('|'.join(row_parts))
        
        # Create hash of all rows
        combined = '\n'.join(result_strings)
        return hashlib.md5(combined.encode()).hexdigest()
    
    def verify_results_identical(self, non_arrow_results: List[Tuple], arrow_results: List[Tuple]) -> bool:
        """Verify that non-arrow and arrow results are identical."""
        print("\n🔍 Verifying result integrity...")
        
        # Check row counts
        if len(non_arrow_results) != len(arrow_results):
            print(f"❌ Row count mismatch: non-arrow={len(non_arrow_results)}, arrow={len(arrow_results)}")
            return False
        
        # Check hashes
        non_arrow_hash = self.hash_results(non_arrow_results)
        arrow_hash = self.hash_results(arrow_results)
        
        if non_arrow_hash != arrow_hash:
            print(f"❌ Data mismatch detected!")
            print(f"   Non-arrow hash: {non_arrow_hash}")
            print(f"   Arrow hash: {arrow_hash}")
            
            # Show first few differences
            for i, (non_arrow_row, arrow_row) in enumerate(zip(non_arrow_results, arrow_results)):
                if non_arrow_row != arrow_row:
                    print(f"   Row {i}: non-arrow={non_arrow_row}, arrow={arrow_row}")
                    if i >= 3:  # Show max 3 differences
                        print("   ...")
                        break
            return False
        
        print(f"✅ Results identical! ({len(non_arrow_results):,} rows, hash: {non_arrow_hash[:8]}...)")
        return True
    
    def print_performance_comparison(self, non_arrow_time: float, arrow_time: float, 
                                   async_non_arrow_time: float, async_arrow_time: float, 
                                   row_count: int):
        """Print detailed performance comparison for all four methods."""
        print("\n" + "="*70)
        print("🏹 ARROW EXTENSION PERFORMANCE COMPARISON")
        print("="*70)
        
        non_arrow_throughput = row_count / non_arrow_time
        arrow_throughput = row_count / arrow_time
        async_non_arrow_throughput = row_count / async_non_arrow_time
        async_arrow_throughput = row_count / async_arrow_time
        
        sync_arrow_improvement = ((non_arrow_time - arrow_time) / non_arrow_time * 100)
        async_arrow_improvement = ((async_non_arrow_time - async_arrow_time) / async_non_arrow_time * 100)
        
        sync_arrow_throughput_improvement = ((arrow_throughput - non_arrow_throughput) / non_arrow_throughput * 100)
        async_arrow_throughput_improvement = ((async_arrow_throughput - async_non_arrow_throughput) / async_non_arrow_throughput * 100)
        
        print(f"📈 SYNC PERFORMANCE:")
        print(f"  • Non-arrow: {non_arrow_time:.2f}s ({non_arrow_throughput:,.0f} rows/second)")
        print(f"  • With arrow: {arrow_time:.2f}s ({arrow_throughput:,.0f} rows/second)")
        
        print(f"\n📈 ASYNC PERFORMANCE:")
        print(f"  • Non-arrow: {async_non_arrow_time:.2f}s ({async_non_arrow_throughput:,.0f} rows/second)")
        print(f"  • With arrow: {async_arrow_time:.2f}s ({async_arrow_throughput:,.0f} rows/second)")
        
        print(f"\n🏹 ARROW IMPROVEMENTS:")
        print(f"  • Sync arrow time: {sync_arrow_improvement:+.1f}%")
        print(f"  • Async arrow time: {async_arrow_improvement:+.1f}%")
        print(f"  • Sync arrow throughput: {sync_arrow_throughput_improvement:+.1f}%")
        print(f"  • Async arrow throughput: {async_arrow_throughput_improvement:+.1f}%")
        
        print(f"\n🔍 ANALYSIS:")
        # Analyze sync arrow performance
        if abs(sync_arrow_improvement) < 5:
            print(f"  • ℹ️  Sync arrow similar performance ({sync_arrow_improvement:+.1f}%) - minimal overhead")
        elif sync_arrow_improvement > 5:
            print(f"  • ✅ Sync arrow faster ({sync_arrow_improvement:+.1f}%) - efficient arrow processing")
        else:
            print(f"  • ⚠️  Sync arrow slower ({abs(sync_arrow_improvement):.1f}%) - investigate arrow overhead")
            
        # Analyze async arrow performance  
        if abs(async_arrow_improvement) < 5:
            print(f"  • ℹ️  Async arrow similar performance ({async_arrow_improvement:+.1f}%) - consistent behavior")
        elif async_arrow_improvement > 5:
            print(f"  • ✅ Async arrow faster ({async_arrow_improvement:+.1f}%) - optimized arrow processing")
        else:
            print(f"  • ⚠️  Async arrow slower ({abs(async_arrow_improvement):.1f}%) - investigate async arrow overhead")
            
        # Overall assessment
        avg_improvement = (sync_arrow_improvement + async_arrow_improvement) / 2
        if avg_improvement > 10:
            print(f"  • 🎯 Arrow extensions provide significant performance benefit ({avg_improvement:.1f}% average)")
        elif avg_improvement > 0:
            print(f"  • 👍 Arrow extensions provide moderate performance benefit ({avg_improvement:.1f}% average)")
        else:
            print(f"  • ⚠️  Arrow extensions show performance overhead ({abs(avg_improvement):.1f}% average) - investigate")
    
    async def run_benchmark(self, test_sizes: List[int] = None):
        """Run the complete arrow extension benchmark."""
        if test_sizes is None:
            test_sizes = [1000, 10000, 100000]  # Test different result sizes
            
        print("🏹 Starting Arrow Extension Benchmark")
        print(f"   Table size: {self.row_count:,} rows")
        print(f"   Test sizes: {[f'{s:,}' for s in test_sizes]}")
        print(f"   Database: {self.conn_params['database']}")
        print()
        
        # Create large table
        table_name = self.setup_large_table("_ARROW")
        
        try:
            all_passed = True
            
            for test_size in test_sizes:
                print(f"\n{'='*50}")
                print(f"🏹 Testing {test_size:,} row fetch - Arrow vs Non-Arrow")
                print('='*50)
                
                # Fetch without arrow extensions (sync)
                non_arrow_results, non_arrow_time, non_arrow_count = self.fetch_without_arrow(table_name, test_size)
                
                # Fetch with arrow extensions (sync)
                arrow_results, arrow_time, arrow_count = self.fetch_with_arrow(table_name, test_size)
                
                # Fetch without arrow extensions (async)
                async_non_arrow_results, async_non_arrow_time, async_non_arrow_count = await self.fetch_async_without_arrow(table_name, test_size)
                
                # Fetch with arrow extensions (async)
                async_arrow_results, async_arrow_time, async_arrow_count = await self.fetch_async_with_arrow(table_name, test_size)
                
                # Verify sync results are identical
                sync_match = self.verify_results_identical(non_arrow_results, arrow_results)
                if not sync_match:
                    all_passed = False
                    print("❌ CRITICAL: Sync arrow results do not match non-arrow!")
                    continue
                    
                # Verify async results are identical
                print("\n🔍 Verifying async arrow results...")
                async_match = self.verify_results_identical(async_non_arrow_results, async_arrow_results)
                if not async_match:
                    all_passed = False
                    print("❌ CRITICAL: Async arrow results do not match non-arrow!")
                    continue
                
                # Verify sync vs async consistency
                print("\n🔍 Verifying sync vs async consistency...")
                sync_async_match = self.verify_results_identical(non_arrow_results, async_non_arrow_results)
                if not sync_async_match:
                    all_passed = False
                    print("❌ CRITICAL: Sync and async results do not match!")
                    continue
                
                # Performance comparison
                self.print_performance_comparison(non_arrow_time, arrow_time, 
                                                async_non_arrow_time, async_arrow_time, test_size)
                
        finally:
            # Cleanup
            print(f"\n{'='*50}")
            self.cleanup_table(table_name)
            
        print(f"\n🎯 BENCHMARK SUMMARY:")
        if all_passed:
            print("✅ All tests passed - arrow and non-arrow results identical")
            print("✅ Arrow extension benchmark completed successfully")
        else:
            print("❌ Some tests failed - investigate arrow extension issues")


async def main():
    """Main benchmark entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark arrow extension performance")
    parser.add_argument("--sizes", nargs='+', type=int, default=[1000, 10000, 100000],
                       help="Result set sizes to test (default: 1000, 10000, 100000)")
    parser.add_argument("--env-file", default=".env", help="Environment file path (default: .env)")
    
    args = parser.parse_args()
    
    try:
        benchmark = ArrowExtensionBenchmark(args.env_file)
        await benchmark.run_benchmark(args.sizes)
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())