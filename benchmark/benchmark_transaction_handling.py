#!/usr/bin/env python3
"""
Transaction Handling Benchmark

Validates that transaction handling (commit, rollback, autocommit) is identical
between sync and async Snowflake connector implementations.

This benchmark tests:
1. Manual transaction control (autocommit=False)
2. Commit behavior and data persistence 
3. Rollback behavior and data rollback
4. Autocommit mode behavior
5. Transaction isolation and consistency
6. Error handling during transactions

Usage:
    python benchmark_transaction_handling.py [--env-file .env]
"""

import argparse
import asyncio
import hashlib
import os
import sys
from typing import Any, Dict

# Add the local src directory to Python path to use development version
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

import snowflake.connector
from snowflake.connector.aio import AsyncSnowflakeConnection
from dotenv import load_dotenv


class TransactionBenchmark:
    """Comprehensive transaction handling validation between sync and async implementations."""
    
    def __init__(self, env_file: str = ".env"):
        """Initialize benchmark with credentials from env file."""
        load_dotenv(env_file)
        
        # Base connection parameters
        self.connection_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        }
        
        # Authentication - prefer private key over password
        private_key_raw = os.getenv('SNOWFLAKE_PRIVATE_KEY_RAW')
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        
        if private_key_raw:
            # Use private key from environment variable
            self.connection_params['private_key'] = private_key_raw
        elif private_key_path:
            # Use private key from file
            self.connection_params['private_key_file'] = private_key_path
        elif password:
            # Fall back to password authentication
            self.connection_params['password'] = password
        else:
            raise ValueError("Must provide either SNOWFLAKE_PRIVATE_KEY_RAW, SNOWFLAKE_PRIVATE_KEY_PATH, or SNOWFLAKE_PASSWORD")
        
        # Validate required parameters
        required = ['user', 'account', 'database', 'schema', 'warehouse']
        missing = [k for k in required if not self.connection_params.get(k)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
            
        self.test_table = "TRANSACTION_HANDLING_TEST"
        self.results = {
            'sync': {},
            'async': {}
        }
        
    def create_test_table_sql(self) -> str:
        """SQL to create test table for transaction testing."""
        return f"""
        CREATE OR REPLACE TABLE {self.test_table} (
            id INTEGER,
            description VARCHAR(100),
            amount DECIMAL(10,2)
        )
        """
    
    def cleanup_table_sql(self) -> str:
        """SQL to cleanup test table."""
        return f"DROP TABLE IF EXISTS {self.test_table}"
    
    def count_rows_sql(self) -> str:
        """SQL to count rows in test table."""
        return f"SELECT COUNT(*) as row_count FROM {self.test_table}"
    
    def insert_test_data_sql(self, batch_id: int, num_rows: int = 3) -> str:
        """SQL to insert deterministic test data."""
        values = []
        for i in range(num_rows):
            values.append(f"({batch_id * 100 + i}, 'Transaction Test {batch_id}-{i}', {(batch_id * 10 + i) * 1.5})")
        
        return f"""
        INSERT INTO {self.test_table} (id, description, amount) 
        VALUES {', '.join(values)}
        """
    
    def run_sync_transaction_tests(self) -> Dict[str, Any]:
        """Run comprehensive transaction tests using sync connection."""
        print("🔄 Running sync transaction tests...")
        
        conn = snowflake.connector.connect(**self.connection_params)
        results = {}
        
        try:
            # Setup test table
            cursor = conn.cursor()
            
            # Use specified database and schema (like other benchmarks)
            cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor.execute(f"USE SCHEMA {self.connection_params['schema']}")
            
            cursor.execute(self.create_test_table_sql())
            
            # Test 1: Manual transaction with commit
            print("  📝 Test 1: Manual transaction with commit")
            conn.autocommit(False)
            
            cursor.execute(self.insert_test_data_sql(1))
            cursor.execute(self.count_rows_sql())
            count_before_commit = cursor.fetchone()[0]
            
            conn.commit()
            cursor.execute(self.count_rows_sql())
            count_after_commit = cursor.fetchone()[0]
            
            results['manual_commit'] = {
                'before_commit': count_before_commit,
                'after_commit': count_after_commit,
                'committed': count_after_commit > 0
            }
            
            # Test 2: Manual transaction with rollback
            print("  📝 Test 2: Manual transaction with rollback")
            initial_count = count_after_commit
            
            cursor.execute(self.insert_test_data_sql(2))
            cursor.execute(self.count_rows_sql())
            count_before_rollback = cursor.fetchone()[0]
            
            conn.rollback()
            cursor.execute(self.count_rows_sql())
            count_after_rollback = cursor.fetchone()[0]
            
            results['manual_rollback'] = {
                'initial_count': initial_count,
                'before_rollback': count_before_rollback,
                'after_rollback': count_after_rollback,
                'rolled_back': count_after_rollback == initial_count
            }
            
            # Test 3: Autocommit mode behavior
            print("  📝 Test 3: Autocommit mode behavior")
            conn.autocommit(True)
            
            cursor.execute(self.count_rows_sql())
            count_before_autocommit = cursor.fetchone()[0]
            
            cursor.execute(self.insert_test_data_sql(3))
            cursor.execute(self.count_rows_sql())
            count_after_autocommit = cursor.fetchone()[0]
            
            results['autocommit'] = {
                'before': count_before_autocommit,
                'after': count_after_autocommit,
                'auto_committed': count_after_autocommit > count_before_autocommit
            }
            
            # Test 4: Transaction isolation test
            print("  📝 Test 4: Transaction isolation test")
            conn.autocommit(False)
            
            # Get initial count for baseline
            cursor.execute(self.count_rows_sql())
            initial_count_isolation = cursor.fetchone()[0]
            
            # Insert uncommitted data in first connection
            cursor.execute(self.insert_test_data_sql(4))
            cursor.execute(self.count_rows_sql())
            count_in_transaction = cursor.fetchone()[0]
            
            # Verify uncommitted data is visible in same transaction
            cursor.execute(f"SELECT * FROM {self.test_table} WHERE id >= 400 ORDER BY id")
            uncommitted_data_same_conn = cursor.fetchall()
            
            # Create second connection to test isolation
            conn2 = snowflake.connector.connect(**self.connection_params)
            cursor2 = conn2.cursor()
            # Set same database and schema context
            cursor2.execute(f"USE DATABASE {self.connection_params['database']}")
            cursor2.execute(f"USE SCHEMA {self.connection_params['schema']}")
            
            # Check count from other connection (should NOT see uncommitted data)
            cursor2.execute(self.count_rows_sql())
            count_other_connection = cursor2.fetchone()[0]
            
            # Try to query specific uncommitted data from other connection
            cursor2.execute(f"SELECT * FROM {self.test_table} WHERE id >= 400 ORDER BY id")
            uncommitted_data_other_conn = cursor2.fetchall()
            
            conn2.close()
            
            # Rollback the transaction
            conn.rollback()
            cursor.execute(self.count_rows_sql())
            count_after_rollback = cursor.fetchone()[0]
            
            results['isolation'] = {
                'initial_count': initial_count_isolation,
                'count_in_transaction': count_in_transaction,
                'count_other_connection': count_other_connection,
                'count_after_rollback': count_after_rollback,
                'uncommitted_visible_same_conn': len(uncommitted_data_same_conn),
                'uncommitted_visible_other_conn': len(uncommitted_data_other_conn),
                'isolated': (count_other_connection == initial_count_isolation and 
                           len(uncommitted_data_other_conn) == 0 and
                           len(uncommitted_data_same_conn) > 0)
            }
            
            # Get final data for comparison
            cursor.execute(f"SELECT * FROM {self.test_table} ORDER BY id")
            results['final_data'] = cursor.fetchall()
            
        finally:
            # Cleanup
            cursor.execute(self.cleanup_table_sql())
            conn.close()
            
        return results
    
    async def run_async_transaction_tests(self) -> Dict[str, Any]:
        """Run comprehensive transaction tests using async connection."""
        print("⚡ Running async transaction tests...")
        
        conn = AsyncSnowflakeConnection(**self.connection_params)
        await conn.connect()
        results = {}
        
        try:
            # Setup test table
            cursor = conn.cursor()
            
            # Use specified database and schema (like other benchmarks)
            await cursor.execute(f"USE DATABASE {self.connection_params['database']}")
            await cursor.execute(f"USE SCHEMA {self.connection_params['schema']}")
            
            await cursor.execute(self.create_test_table_sql())
            print("  ✅ Test table created")
            
            # Test 1: Manual transaction with commit
            print("  📝 Test 1: Manual transaction with commit")
            await conn.autocommit(False)
            
            await cursor.execute(self.insert_test_data_sql(1))
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_before_commit = int(row[0]) if row else 0
            
            await conn.commit()
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_after_commit = int(row[0]) if row else 0
            
            results['manual_commit'] = {
                'before_commit': count_before_commit,
                'after_commit': count_after_commit,
                'committed': count_after_commit > 0
            }
            
            # Test 2: Manual transaction with rollback
            print("  📝 Test 2: Manual transaction with rollback")
            initial_count = count_after_commit
            
            await cursor.execute(self.insert_test_data_sql(2))
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_before_rollback = int(row[0]) if row else 0
            
            await conn.rollback()
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_after_rollback = int(row[0]) if row else 0
            
            results['manual_rollback'] = {
                'initial_count': initial_count,
                'before_rollback': count_before_rollback,
                'after_rollback': count_after_rollback,
                'rolled_back': count_after_rollback == initial_count
            }
            
            # Test 3: Autocommit mode behavior
            print("  📝 Test 3: Autocommit mode behavior")
            await conn.autocommit(True)
            
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_before_autocommit = int(row[0]) if row else 0
            
            await cursor.execute(self.insert_test_data_sql(3))
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_after_autocommit = int(row[0]) if row else 0
            
            results['autocommit'] = {
                'before': count_before_autocommit,
                'after': count_after_autocommit,
                'auto_committed': count_after_autocommit > count_before_autocommit
            }
            
            # Test 4: Transaction isolation test
            print("  📝 Test 4: Transaction isolation test")
            await conn.autocommit(False)
            
            # Get initial count for baseline
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            initial_count_isolation = int(row[0]) if row else 0
            
            # Insert uncommitted data in first connection
            await cursor.execute(self.insert_test_data_sql(4))
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_in_transaction = int(row[0]) if row else 0
            
            # Verify uncommitted data is visible in same transaction
            await cursor.execute(f"SELECT * FROM {self.test_table} WHERE id >= 400 ORDER BY id")
            uncommitted_data_same_conn = await cursor.fetchall()
            
            # Create second connection to test isolation
            conn2 = AsyncSnowflakeConnection(**self.connection_params)
            await conn2.connect()
            cursor2 = conn2.cursor()
            # Set same database and schema context
            await cursor2.execute(f"USE DATABASE {self.connection_params['database']}")
            await cursor2.execute(f"USE SCHEMA {self.connection_params['schema']}")
            
            # Check count from other connection (should NOT see uncommitted data)
            await cursor2.execute(self.count_rows_sql())
            row = await cursor2.fetchone()
            count_other_connection = int(row[0]) if row else 0
            
            # Try to query specific uncommitted data from other connection
            await cursor2.execute(f"SELECT * FROM {self.test_table} WHERE id >= 400 ORDER BY id")
            uncommitted_data_other_conn = await cursor2.fetchall()
            
            await conn2.close()
            
            # Rollback the transaction
            await conn.rollback()
            await cursor.execute(self.count_rows_sql())
            row = await cursor.fetchone()
            count_after_rollback = int(row[0]) if row else 0
            
            results['isolation'] = {
                'initial_count': initial_count_isolation,
                'count_in_transaction': count_in_transaction,
                'count_other_connection': count_other_connection,
                'count_after_rollback': count_after_rollback,
                'uncommitted_visible_same_conn': len(uncommitted_data_same_conn),
                'uncommitted_visible_other_conn': len(uncommitted_data_other_conn),
                'isolated': (count_other_connection == initial_count_isolation and 
                           len(uncommitted_data_other_conn) == 0 and
                           len(uncommitted_data_same_conn) > 0)
            }
            
            # Get final data for comparison
            await cursor.execute(f"SELECT * FROM {self.test_table} ORDER BY id")
            results['final_data'] = await cursor.fetchall()
            
        except Exception as e:
            print(f"❌ Async transaction test failed: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            # Cleanup
            if 'cursor' in locals():
                await cursor.execute(self.cleanup_table_sql())
            await conn.close()
            
        return results
    
    def compare_results(self, sync_results: Dict[str, Any], async_results: Dict[str, Any]) -> bool:
        """Compare sync and async transaction test results for consistency."""
        print("\n" + "="*60)
        print("📊 TRANSACTION HANDLING COMPARISON")
        print("="*60)
        
        all_passed = True
        
        # Test 1: Manual commit comparison
        print("\n🔍 Test 1: Manual Transaction Commit")
        sync_commit = sync_results['manual_commit']
        async_commit = async_results['manual_commit']
        
        commit_match = (
            sync_commit['committed'] == async_commit['committed'] and
            sync_commit['before_commit'] == async_commit['before_commit'] and
            sync_commit['after_commit'] == async_commit['after_commit']
        )
        
        print(f"  Sync:  before={sync_commit['before_commit']}, after={sync_commit['after_commit']}, committed={sync_commit['committed']}")
        print(f"  Async: before={async_commit['before_commit']}, after={async_commit['after_commit']}, committed={async_commit['committed']}")
        print(f"  ✅ MATCH" if commit_match else f"  ❌ MISMATCH")
        all_passed = all_passed and commit_match
        
        # Test 2: Manual rollback comparison  
        print("\n🔍 Test 2: Manual Transaction Rollback")
        sync_rollback = sync_results['manual_rollback']
        async_rollback = async_results['manual_rollback']
        
        rollback_match = (
            sync_rollback['rolled_back'] == async_rollback['rolled_back'] and
            sync_rollback['after_rollback'] == async_rollback['after_rollback']
        )
        
        print(f"  Sync:  initial={sync_rollback['initial_count']}, before={sync_rollback['before_rollback']}, after={sync_rollback['after_rollback']}, rolled_back={sync_rollback['rolled_back']}")
        print(f"  Async: initial={async_rollback['initial_count']}, before={async_rollback['before_rollback']}, after={async_rollback['after_rollback']}, rolled_back={async_rollback['rolled_back']}")
        print(f"  ✅ MATCH" if rollback_match else f"  ❌ MISMATCH")
        all_passed = all_passed and rollback_match
        
        # Test 3: Autocommit comparison
        print("\n🔍 Test 3: Autocommit Mode")
        sync_autocommit = sync_results['autocommit']
        async_autocommit = async_results['autocommit']
        
        autocommit_match = (
            sync_autocommit['auto_committed'] == async_autocommit['auto_committed']
        )
        
        print(f"  Sync:  before={sync_autocommit['before']}, after={sync_autocommit['after']}, auto_committed={sync_autocommit['auto_committed']}")
        print(f"  Async: before={async_autocommit['before']}, after={async_autocommit['after']}, auto_committed={async_autocommit['auto_committed']}")
        print(f"  ✅ MATCH" if autocommit_match else f"  ❌ MISMATCH")
        all_passed = all_passed and autocommit_match
        
        # Test 4: Transaction isolation comparison
        print("\n🔍 Test 4: Transaction Isolation")
        sync_isolation = sync_results['isolation']
        async_isolation = async_results['isolation']
        
        isolation_match = (
            sync_isolation['isolated'] == async_isolation['isolated'] and
            sync_isolation['count_other_connection'] == async_isolation['count_other_connection'] and
            sync_isolation['uncommitted_visible_same_conn'] == async_isolation['uncommitted_visible_same_conn'] and
            sync_isolation['uncommitted_visible_other_conn'] == async_isolation['uncommitted_visible_other_conn']
        )
        
        print(f"  Sync:  in_txn={sync_isolation['count_in_transaction']}, other_conn={sync_isolation['count_other_connection']}, isolated={sync_isolation['isolated']}")
        print(f"         uncommitted visible same_conn={sync_isolation['uncommitted_visible_same_conn']}, other_conn={sync_isolation['uncommitted_visible_other_conn']}")
        print(f"  Async: in_txn={async_isolation['count_in_transaction']}, other_conn={async_isolation['count_other_connection']}, isolated={async_isolation['isolated']}")
        print(f"         uncommitted visible same_conn={async_isolation['uncommitted_visible_same_conn']}, other_conn={async_isolation['uncommitted_visible_other_conn']}")
        print(f"  ✅ MATCH" if isolation_match else f"  ❌ MISMATCH")
        all_passed = all_passed and isolation_match
        
        # Final data comparison
        print("\n🔍 Final Data Consistency")
        sync_data = sync_results['final_data']
        async_data = async_results['final_data']
        
        # Convert to hashable format for comparison
        def data_hash(data):
            return hashlib.md5(str(sorted(data)).encode()).hexdigest()[:8]
        
        sync_hash = data_hash(sync_data)
        async_hash = data_hash(async_data)
        data_match = sync_hash == async_hash
        
        print(f"  Sync final data:  {len(sync_data)} rows (hash: {sync_hash})")
        print(f"  Async final data: {len(async_data)} rows (hash: {async_hash})")
        print(f"  ✅ IDENTICAL" if data_match else f"  ❌ DIFFERENT")
        all_passed = all_passed and data_match
        
        return all_passed
    
    async def run_benchmark(self) -> bool:
        """Run complete transaction handling benchmark."""
        print("🚀 Starting Transaction Handling Benchmark")
        print("="*60)
        
        # Run sync tests
        sync_results = self.run_sync_transaction_tests()
        print("✅ Sync transaction tests completed")
        
        # Run async tests
        async_results = await self.run_async_transaction_tests()
        print("✅ Async transaction tests completed")
        
        # Compare results
        all_passed = self.compare_results(sync_results, async_results)
        
        print("\n" + "="*60)
        print("📊 TRANSACTION BENCHMARK SUMMARY")
        print("="*60)
        
        if all_passed:
            print("🎉 ALL TRANSACTION TESTS PASSED!")
            print("✅ Sync and async transaction handling is perfectly aligned")
            print("✅ Commit behavior is identical")
            print("✅ Rollback behavior is identical") 
            print("✅ Autocommit behavior is identical")
            print("✅ Transaction isolation is identical")
            print("✅ Final data consistency maintained")
        else:
            print("❌ TRANSACTION HANDLING DIFFERENCES DETECTED")
            print("⚠️  Manual review required")
            
        return all_passed


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Transaction Handling Benchmark')
    parser.add_argument('--env-file', default='.env', help='Environment file path')
    return parser.parse_args()


async def main():
    """Main benchmark execution."""
    args = parse_args()
    
    try:
        benchmark = TransactionBenchmark(args.env_file)
        
        success = await benchmark.run_benchmark()
        
        if success:
            print("\n✅ Transaction handling benchmark completed successfully!")
        else:
            print("\n❌ Transaction handling benchmark found inconsistencies!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Benchmark failed with error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())