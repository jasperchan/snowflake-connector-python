#!/usr/bin/env python3
"""
Binary Data Debug Script - Isolating sync vs async binary data differences.

This script tests specific binary data scenarios to identify where
sync and async implementations diverge in binary data handling.
"""

import asyncio
import os
import sys
import time
from typing import List, Tuple, Any

# Add the local src directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
src_path = os.path.join(repo_root, 'src')
sys.path.insert(0, src_path)

from dotenv import load_dotenv
import snowflake.connector
import snowflake.connector.aio


def debug_binary_value(value: Any, label: str) -> None:
    """Debug print binary value with type and representation info."""
    print(f"  {label}:")
    print(f"    Type: {type(value)}")
    print(f"    Value: {repr(value)}")
    if isinstance(value, (bytes, bytearray)):
        print(f"    Hex: {value.hex()}")
        print(f"    Length: {len(value)}")
    elif isinstance(value, str):
        print(f"    Length: {len(value)}")
        try:
            # Try to decode as hex
            decoded = bytes.fromhex(value)
            print(f"    As hex bytes: {repr(decoded)}")
        except ValueError:
            print(f"    Not valid hex string")
    print()


async def test_binary_formats():
    """Test different binary data formats between sync and async."""
    load_dotenv()
    
    # Connection parameters
    conn_params = {
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
        conn_params['private_key_file'] = private_key_path
    elif private_key_raw:
        conn_params['private_key'] = private_key_raw
    elif password:
        conn_params['password'] = password
    else:
        raise ValueError("No authentication method configured")

    table_name = "BINARY_DEBUG_TEST"
    
    print("🔍 Binary Data Debug Analysis")
    print("="*50)
    
    # Create test table with simple binary data
    print("🔨 Creating test table with binary data...")
    conn = snowflake.connector.connect(**conn_params)
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {conn_params['database']}")
        cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        
        # Drop and create test table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                ID INTEGER,
                BINARY_HEX BINARY,
                BINARY_SIMPLE BINARY
            ) AS
            SELECT 
                1 as ID,
                HEX_ENCODE('HELLO') as BINARY_HEX,
                TO_BINARY('48454C4C4F') as BINARY_SIMPLE
            UNION ALL
            SELECT 
                2 as ID, 
                HEX_ENCODE('TEST123') as BINARY_HEX,
                TO_BINARY('54455354313233') as BINARY_SIMPLE
        """)
        print(f"✅ Test table '{table_name}' created")
    finally:
        conn.close()
    
    # Test different combinations
    test_scenarios = [
        ("Sync Non-Arrow", lambda: test_sync(conn_params, table_name, use_arrow=False)),
        ("Sync With Arrow", lambda: test_sync(conn_params, table_name, use_arrow=True)),
        ("Async Non-Arrow", lambda: test_async(conn_params, table_name, use_arrow=False)),
        ("Async With Arrow", lambda: test_async(conn_params, table_name, use_arrow=True)),
    ]
    
    results = {}
    
    for scenario_name, test_func in test_scenarios:
        print(f"\n{'='*20} {scenario_name} {'='*20}")
        try:
            if scenario_name.startswith("Async"):
                result = await test_func()
            else:
                result = test_func()
            results[scenario_name] = result
            
            print(f"📊 Results for {scenario_name}:")
            for i, row in enumerate(result):
                print(f"  Row {i+1}: ID={row[0]}")
                debug_binary_value(row[1], "BINARY_HEX")
                debug_binary_value(row[2], "BINARY_SIMPLE")
                
        except Exception as e:
            print(f"❌ Error in {scenario_name}: {e}")
            results[scenario_name] = None
    
    # Compare results
    print(f"\n{'='*20} COMPARISON ANALYSIS {'='*20}")
    
    # Compare sync vs async for same arrow setting
    compare_results("Sync Non-Arrow", "Async Non-Arrow", results)
    compare_results("Sync With Arrow", "Async With Arrow", results)
    
    # Compare arrow vs non-arrow for same sync/async
    compare_results("Sync Non-Arrow", "Sync With Arrow", results)
    compare_results("Async Non-Arrow", "Async With Arrow", results)
    
    # Cleanup
    print(f"\n🧹 Cleaning up test table...")
    conn = snowflake.connector.connect(**conn_params)
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {conn_params['database']}")
        cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✅ Test table '{table_name}' cleaned up")
    finally:
        conn.close()


def test_sync(conn_params: dict, table_name: str, use_arrow: bool) -> List[Tuple]:
    """Test sync connector with specified arrow setting."""
    params = conn_params.copy()
    params['use_arrow_result_format'] = use_arrow
    
    conn = snowflake.connector.connect(**params)
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {conn_params['database']}")
        cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        
        cursor.execute(f"SELECT ID, BINARY_HEX, BINARY_SIMPLE FROM {table_name} ORDER BY ID")
        return cursor.fetchall()
    finally:
        conn.close()


async def test_async(conn_params: dict, table_name: str, use_arrow: bool) -> List[Tuple]:
    """Test async connector with specified arrow setting."""
    params = conn_params.copy()
    params['use_arrow_result_format'] = use_arrow
    
    conn = await snowflake.connector.aio.connect(**params)
    try:
        cursor = conn.cursor()
        await cursor.execute(f"USE DATABASE {conn_params['database']}")
        await cursor.execute(f"USE SCHEMA {conn_params['schema']}")
        
        await cursor.execute(f"SELECT ID, BINARY_HEX, BINARY_SIMPLE FROM {table_name} ORDER BY ID")
        return await cursor.fetchall()
    finally:
        await conn.close()


def compare_results(name1: str, name2: str, results: dict) -> None:
    """Compare two result sets and highlight differences."""
    result1 = results.get(name1)
    result2 = results.get(name2)
    
    print(f"\n🔍 Comparing {name1} vs {name2}:")
    
    if result1 is None or result2 is None:
        print("  ❌ Cannot compare - one or both results missing")
        return
    
    if len(result1) != len(result2):
        print(f"  ❌ Row count mismatch: {len(result1)} vs {len(result2)}")
        return
    
    differences_found = False
    
    for i, (row1, row2) in enumerate(zip(result1, result2)):
        print(f"  Row {i+1}:")
        
        # Compare each column
        for j, (val1, val2) in enumerate(zip(row1, row2)):
            col_names = ["ID", "BINARY_HEX", "BINARY_SIMPLE"]
            col_name = col_names[j] if j < len(col_names) else f"COL_{j}"
            
            if val1 == val2:
                print(f"    ✅ {col_name}: Identical")
            else:
                print(f"    ❌ {col_name}: DIFFERENT")
                print(f"      {name1}: {type(val1).__name__} = {repr(val1)}")
                print(f"      {name2}: {type(val2).__name__} = {repr(val2)}")
                differences_found = True
    
    if not differences_found:
        print("  ✅ All values identical!")


if __name__ == "__main__":
    asyncio.run(test_binary_formats())