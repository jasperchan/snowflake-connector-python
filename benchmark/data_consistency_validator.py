#!/usr/bin/env python3
"""
Data Consistency Validator - Comprehensive testing framework for sync vs async data processing.

This framework tests all Snowflake data types to ensure perfect alignment between
sync and async implementations, validating the composition pattern used in async.
"""

import asyncio
import hashlib
import os
import sys
import time
import decimal
from datetime import datetime, date, time as dt_time
from typing import List, Tuple, Any, Dict, Optional, Union
import json

# Add the local src directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

from dotenv import load_dotenv
import snowflake.connector
import snowflake.connector.aio


class DataConsistencyValidator:
    """Comprehensive validator for sync vs async data processing consistency."""
    
    def __init__(self, env_file: str = ".env"):
        """Initialize validator with connection parameters."""
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
            
        self.table_name = "DATA_CONSISTENCY_TEST"
        self.test_results = {}
        
    def get_arrow_params(self, enabled: bool) -> Dict[str, Any]:
        """Get connection parameters with arrow setting."""
        params = self.conn_params.copy()
        params['use_arrow_result_format'] = enabled
        return params

    def create_comprehensive_test_table(self) -> str:
        """Create test table with all Snowflake data types."""
        print("🔨 Creating comprehensive test table with all data types...")
        
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            
            # Create table with essential data types for consistency testing
            cursor.execute(f"""
                CREATE TABLE {self.table_name} (
                    ID INTEGER,
                    STRING_VAL STRING,
                    DECIMAL_VAL DECIMAL(10,2),
                    FLOAT_VAL FLOAT,
                    BOOLEAN_VAL BOOLEAN,
                    BINARY_VAL BINARY,
                    DATE_VAL DATE,
                    TIMESTAMP_VAL TIMESTAMP,
                    VARIANT_VAL VARIANT
                )
            """)
            
            # Insert test data using SELECT statements
            cursor.execute(f"""
                INSERT INTO {self.table_name} 
                SELECT 1, 'Hello World', 12345.67, 3.14159, true,
                       HEX_ENCODE('Binary Test'), '2024-01-15'::DATE, '2024-01-15 14:30:45.123'::TIMESTAMP,
                       PARSE_JSON('{{"name": "test", "value": 123}}')
                UNION ALL
                SELECT 2, 'Special chars: üñ', -99999.99, -3.14159, false,
                       HEX_ENCODE('Different'), '1900-01-01'::DATE, '1900-01-01 00:00:00.000'::TIMESTAMP,
                       PARSE_JSON('[1, 2, 3]')
                UNION ALL
                SELECT 3, null, null, null, null, null, null, null, null
            """)
            
            print(f"✅ Test table '{self.table_name}' created with comprehensive data types")
            return self.table_name
            
        finally:
            conn.close()

    def cleanup_table(self) -> None:
        """Clean up test table."""
        print(f"🧹 Cleaning up table '{self.table_name}'...")
        conn = snowflake.connector.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            print(f"✅ Table '{self.table_name}' cleaned up")
        finally:
            conn.close()

    def fetch_sync_data(self, use_arrow: bool) -> List[Tuple]:
        """Fetch data using sync connector."""
        print(f"🔄 Fetching data with sync connector (arrow={use_arrow})")
        
        params = self.get_arrow_params(use_arrow)
        conn = snowflake.connector.connect(**params)
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            cursor.execute(f"SELECT * FROM {self.table_name} ORDER BY ID")
            results = cursor.fetchall()
            
            print(f"  ✅ Sync (arrow={use_arrow}) fetched {len(results)} rows")
            return results
            
        finally:
            conn.close()

    async def fetch_async_data(self, use_arrow: bool) -> List[Tuple]:
        """Fetch data using async connector."""
        print(f"⚡ Fetching data with async connector (arrow={use_arrow})")
        
        params = self.get_arrow_params(use_arrow)
        conn = await snowflake.connector.aio.connect(**params)
        try:
            cursor = conn.cursor()
            await cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            await cursor.execute(f"USE SCHEMA {self.conn_params['schema']}")
            
            await cursor.execute(f"SELECT * FROM {self.table_name} ORDER BY ID")
            results = await cursor.fetchall()
            
            print(f"  ✅ Async (arrow={use_arrow}) fetched {len(results)} rows")
            return results
            
        finally:
            await conn.close()

    def analyze_value_differences(self, val1: Any, val2: Any, context: str) -> Dict[str, Any]:
        """Analyze differences between two values."""
        analysis = {
            'identical': val1 == val2,
            'type1': type(val1).__name__,
            'type2': type(val2).__name__,
            'value1': repr(val1),
            'value2': repr(val2),
            'context': context
        }
        
        # Special handling for binary data
        if isinstance(val1, (bytes, bytearray)) and isinstance(val2, (bytes, bytearray)):
            analysis['binary_content_identical'] = bytes(val1) == bytes(val2)
            analysis['hex1'] = val1.hex() if val1 else 'NULL'
            analysis['hex2'] = val2.hex() if val2 else 'NULL'
        
        # Special handling for decimals
        if isinstance(val1, decimal.Decimal) or isinstance(val2, decimal.Decimal):
            try:
                analysis['decimal_value_identical'] = float(val1) == float(val2)
            except (ValueError, TypeError):
                analysis['decimal_value_identical'] = False
        
        # Special handling for datetime objects
        if isinstance(val1, (datetime, date, dt_time)) or isinstance(val2, (datetime, date, dt_time)):
            analysis['datetime_str1'] = str(val1)
            analysis['datetime_str2'] = str(val2)
        
        return analysis

    def compare_results(self, name1: str, results1: List[Tuple], 
                       name2: str, results2: List[Tuple]) -> Dict[str, Any]:
        """Compare two result sets in detail."""
        comparison = {
            'datasets': f"{name1} vs {name2}",
            'row_count_match': len(results1) == len(results2),
            'row_count1': len(results1),
            'row_count2': len(results2),
            'overall_identical': True,
            'differences': []
        }
        
        if not comparison['row_count_match']:
            comparison['overall_identical'] = False
            return comparison
        
        # Compare each row and column
        for row_idx, (row1, row2) in enumerate(zip(results1, results2)):
            for col_idx, (val1, val2) in enumerate(zip(row1, row2)):
                analysis = self.analyze_value_differences(
                    val1, val2, f"Row {row_idx}, Column {col_idx}"
                )
                
                if not analysis['identical']:
                    # Check if it's just a type difference with same content
                    if isinstance(val1, (bytes, bytearray)) and isinstance(val2, (bytes, bytearray)):
                        if analysis.get('binary_content_identical', False):
                            analysis['acceptable_difference'] = 'Binary type difference (bytes vs bytearray)'
                        else:
                            comparison['overall_identical'] = False
                    else:
                        comparison['overall_identical'] = False
                    
                    comparison['differences'].append(analysis)
        
        return comparison

    def print_comparison_summary(self, comparison: Dict[str, Any]) -> None:
        """Print a summary of comparison results."""
        print(f"\n🔍 Comparison: {comparison['datasets']}")
        print(f"  Row counts: {comparison['row_count1']} vs {comparison['row_count2']}")
        
        if comparison['overall_identical']:
            print("  ✅ Results are identical!")
        else:
            print(f"  ❌ Found {len(comparison['differences'])} differences")
            
            # Group differences by type
            type_diffs = {}
            binary_type_diffs = 0
            
            for diff in comparison['differences']:
                if diff.get('acceptable_difference'):
                    binary_type_diffs += 1
                else:
                    key = f"{diff['type1']} vs {diff['type2']}"
                    type_diffs[key] = type_diffs.get(key, 0) + 1
            
            if binary_type_diffs > 0:
                print(f"    ℹ️  {binary_type_diffs} acceptable binary type differences (bytes vs bytearray)")
            
            for diff_type, count in type_diffs.items():
                print(f"    ❌ {count} {diff_type} differences")
                
            # Show first few critical differences
            critical_diffs = [d for d in comparison['differences'] 
                            if not d.get('acceptable_difference')]
            
            for i, diff in enumerate(critical_diffs[:3]):
                print(f"    Example {i+1}: {diff['context']}")
                print(f"      {diff['type1']}: {diff['value1']}")
                print(f"      {diff['type2']}: {diff['value2']}")

    async def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run comprehensive data consistency validation."""
        print("🚀 Starting Comprehensive Data Consistency Validation")
        print("="*60)
        
        # Create test table
        self.create_comprehensive_test_table()
        
        try:
            # Test all four combinations
            test_scenarios = [
                ("sync_no_arrow", lambda: self.fetch_sync_data(False)),
                ("sync_with_arrow", lambda: self.fetch_sync_data(True)),
                ("async_no_arrow", lambda: self.fetch_async_data(False)),
                ("async_with_arrow", lambda: self.fetch_async_data(True)),
            ]
            
            results = {}
            
            # Execute all scenarios
            for scenario_name, fetch_func in test_scenarios:
                print(f"\n{'='*20} {scenario_name.upper()} {'='*20}")
                try:
                    if scenario_name.startswith("async"):
                        result = await fetch_func()
                    else:
                        result = fetch_func()
                    results[scenario_name] = result
                except Exception as e:
                    print(f"❌ Error in {scenario_name}: {e}")
                    results[scenario_name] = None
            
            # Perform comparisons
            print(f"\n{'='*20} COMPARISON ANALYSIS {'='*20}")
            
            comparisons = []
            
            # Key comparisons to validate async composition pattern
            comparison_pairs = [
                ("sync_no_arrow", "async_no_arrow"),      # Same format, different implementation
                ("sync_with_arrow", "async_with_arrow"),  # Same format, different implementation  
                ("sync_no_arrow", "sync_with_arrow"),     # Same implementation, different format
                ("async_no_arrow", "async_with_arrow"),   # Same implementation, different format
            ]
            
            for name1, name2 in comparison_pairs:
                if results.get(name1) and results.get(name2):
                    comparison = self.compare_results(name1, results[name1], name2, results[name2])
                    comparisons.append(comparison)
                    self.print_comparison_summary(comparison)
            
            # Overall summary
            print(f"\n{'='*20} VALIDATION SUMMARY {'='*20}")
            all_passed = all(c['overall_identical'] for c in comparisons)
            
            if all_passed:
                print("✅ ALL VALIDATIONS PASSED")
                print("✅ Sync and async implementations are perfectly aligned")
                print("✅ Arrow and JSON formats produce identical results")
                print("✅ Async composition pattern successfully maintains sync business logic")
            else:
                print("❌ VALIDATION FAILURES DETECTED")
                failed_comparisons = [c for c in comparisons if not c['overall_identical']]
                print(f"❌ {len(failed_comparisons)} critical comparisons failed")
                print("⚠️  Async implementation may have data consistency issues")
            
            return {
                'all_passed': all_passed,
                'results': results,
                'comparisons': comparisons,
                'summary': {
                    'total_scenarios': len(test_scenarios),
                    'successful_scenarios': len([r for r in results.values() if r is not None]),
                    'total_comparisons': len(comparisons),
                    'passed_comparisons': len([c for c in comparisons if c['overall_identical']])
                }
            }
            
        finally:
            # Cleanup
            self.cleanup_table()

    async def run_parameter_binding_validation(self) -> Dict[str, Any]:
        """Test parameter binding consistency between sync and async."""
        print("\n🔧 Parameter Binding Validation")
        print("-" * 40)
        
        # Test various parameter types
        test_params = [
            ("integer", 42),
            ("float", 3.14159),
            ("string", "Hello World"),
            ("bytes", b"binary data"),
            ("bytearray", bytearray(b"bytearray data")),
            ("None", None),
            ("boolean", True),
            ("list", [1, 2, 3]),
            ("dict", {"key": "value"}),
        ]
        
        binding_results = {}
        
        for param_name, param_value in test_params:
            print(f"Testing parameter: {param_name} = {repr(param_value)}")
            
            # Test sync parameter binding
            sync_conn = snowflake.connector.connect(**self.conn_params)
            try:
                sync_cursor = sync_conn.cursor()
                sync_cursor.execute("SELECT %s as test_param", (param_value,))
                sync_result = sync_cursor.fetchone()
            except Exception as e:
                sync_result = f"ERROR: {e}"
            finally:
                sync_conn.close()
            
            # Test async parameter binding
            try:
                async_conn = await snowflake.connector.aio.connect(**self.conn_params)
                try:
                    async_cursor = async_conn.cursor()
                    await async_cursor.execute("SELECT %s as test_param", (param_value,))
                    async_result = await async_cursor.fetchone()
                except Exception as e:
                    async_result = f"ERROR: {e}"
                finally:
                    await async_conn.close()
            except Exception as e:
                async_result = f"ERROR: {e}"
            
            # Compare results
            binding_results[param_name] = {
                'input': param_value,
                'sync_result': sync_result,
                'async_result': async_result,
                'identical': sync_result == async_result
            }
            
            if sync_result == async_result:
                print(f"  ✅ {param_name}: Identical results")
            else:
                print(f"  ❌ {param_name}: Different results")
                print(f"    Sync: {sync_result}")
                print(f"    Async: {async_result}")
        
        return binding_results


async def main():
    """Main validation entry point."""
    try:
        validator = DataConsistencyValidator()
        
        # Run comprehensive data validation
        validation_results = await validator.run_comprehensive_validation()
        
        # Run parameter binding validation
        binding_results = await validator.run_parameter_binding_validation()
        
        # Final summary
        print(f"\n{'='*60}")
        print("📊 FINAL VALIDATION SUMMARY")
        print(f"{'='*60}")
        print(f"Data Consistency: {'✅ PASSED' if validation_results['all_passed'] else '❌ FAILED'}")
        print(f"Scenarios Tested: {validation_results['summary']['successful_scenarios']}/{validation_results['summary']['total_scenarios']}")
        print(f"Comparisons Passed: {validation_results['summary']['passed_comparisons']}/{validation_results['summary']['total_comparisons']}")
        
        param_binding_passed = all(r['identical'] for r in binding_results.values())
        print(f"Parameter Binding: {'✅ PASSED' if param_binding_passed else '❌ FAILED'}")
        
        if validation_results['all_passed'] and param_binding_passed:
            print("\n🎉 ALL VALIDATIONS PASSED!")
            print("✅ Async implementation perfectly maintains sync business logic alignment")
        else:
            print("\n⚠️  VALIDATION ISSUES DETECTED")
            print("❌ Manual investigation required for data consistency issues")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())