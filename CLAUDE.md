# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Snowflake Connector for Python, which implements the Python DB API 2.0 specification for connecting to Snowflake data warehouses. The connector supports Python 3.9+ and has no dependencies on JDBC or ODBC.

## Common Development Commands

### Building the Project

```bash
# Local build with PEP-517
python -m pip install -U pip setuptools wheel build
python -m build --wheel .
# Output: ./dist/snowflake_connector_python*.whl

# Docker build (all Python versions)
ci/build_docker.sh
# Output: dist/repaired_wheels/

# Docker build (specific versions)
ci/build_docker.sh "3.9 3.10"
```

### Running Tests

Before running tests, create `test/parameters.py` with connection parameters:
```python
CONNECTION_PARAMETERS = {
    'account':  'testaccount',
    'user':     'user',
    'password': 'testpasswd',
    'schema':   'testschema',
    'database': 'testdb',
}
```

```bash
# Run linting and unit/integration tests
tox -e "fix_lint,py39{,-pandas,-sso}"

# Run specific test types
tox -e py39-unit           # Unit tests only
tox -e py39-integ          # Integration tests only
tox -e py39-unit-integ     # Both unit and integration
tox -e py39-pandas         # Pandas tests
tox -e py39-sso            # SSO tests
tox -e py39-unit-parallel  # Run tests in parallel

# Run a single test
SINGLE_TEST_NAME="test/integ/test_connection.py::test_basic" tox -e py39-single

# Or activate tox environment directly
. .tox/py39/bin/activate
pytest -v test/integ/test_connection.py::test_basic
```

### Linting and Code Style

```bash
# Run pre-commit hooks (includes Black formatter, Flake8, etc.)
tox -e fix_lint

# The project uses Black for code formatting
```

### Other Utilities

```bash
# Generate coverage report
tox -e coverage

# Check for dependency conflicts
tox -e dependency

# Create development virtualenv
tox --devenv venv39 -e py39
. venv39/bin/activate
```

## Architecture Overview

### Core Components

1. **Connection Management** (`src/snowflake/connector/connection.py`)
   - `SnowflakeConnection` class manages database connections, sessions, and authentication
   - Handles connection pooling, retry logic, and session heartbeat
   - Entry point: `snowflake.connector.connect()`

2. **Cursor Operations** (`src/snowflake/connector/cursor.py`)
   - `SnowflakeCursor` implements DB-API 2.0 cursor interface
   - Executes queries, fetches results, handles file transfers (GET/PUT)
   - Supports multiple result formats (JSON, Arrow)

3. **Network Layer** (`src/snowflake/connector/network.py`)
   - `SnowflakeRestful` manages REST API communication
   - Handles request/response processing, compression, and retries
   - All Snowflake API calls go through `_request_exec()`

4. **Authentication System** (`src/snowflake/connector/auth/`)
   - Plugin-based architecture with `Auth` base class
   - Supports multiple methods: password, key-pair, OAuth, browser SSO, Okta, MFA
   - Authentication flow selected based on connection parameters
   - Key authenticators: `AuthByDefault`, `AuthByKeyPair`, `AuthByWebBrowser`, `AuthByOAuth`

5. **Data Type Conversion** (`src/snowflake/connector/converter.py`)
   - `SnowflakeConverter` handles Python ↔ Snowflake type conversions
   - Manages timezone conversions, binary encoding, precision/scale
   - Optional NumPy/Pandas integration for performance

6. **Cloud Storage Integration**
   - `S3StorageClient` (AWS), `AzureStorageClient` (Azure), `GCSStorageClient` (GCP)
   - All inherit from `SnowflakeStorageClient` base class
   - Handle file uploads/downloads for COPY operations with encryption and compression

7. **File Transfer** (`src/snowflake/connector/file_transfer_agent.py`)
   - Orchestrates GET/PUT commands for bulk data operations
   - Manages parallel transfers, progress tracking, and error handling

### Key Design Patterns

- **Vendored Dependencies**: Critical dependencies (requests, urllib3) are vendored in `src/snowflake/connector/vendored/`
- **Optional Dependencies**: Arrow and Pandas support are loaded on-demand
- **Retry Mechanisms**: Exponential backoff policies for network operations
- **Thread Safety**: Connection objects use locks for thread-safe operations
- **Telemetry**: Built-in usage and performance tracking (can be disabled)
- **OCSP Validation**: Certificate validation with configurable fail-open/fail-closed modes

### Important Files and Directories

- `src/snowflake/connector/__init__.py` - Main module exports and DB-API compliance
- `src/snowflake/connector/errors.py` - Exception hierarchy
- `src/snowflake/connector/constants.py` - Configuration constants
- `src/snowflake/connector/nanoarrow_cpp/` - C++ extensions for Arrow format
- `test/` - Test suite organized by test type (unit/, integ/, pandas/)

### Testing Considerations

- Use pytest markers: `unit`, `integ`, `pandas`, `sso`, `lambda`, `aws`, `azure`, `gcp`
- Set `cloud_provider` environment variable to match your test account
- Integration tests require valid Snowflake account credentials
- Pandas/Arrow tests require optional dependencies installed

## aiohttp-to-requests Error Mapping Requirements

### Critical Compatibility Requirements

When migrating from `requests` to `aiohttp`, the following structural differences must be addressed:

#### 1. Response Attribute Differences
- **aiohttp**: `response.status` (integer)
- **requests**: `response.status_code` (integer)
- **Impact**: All code expects `.status_code` - tests, error handlers, storage clients

#### 2. Exception Structure Requirements
From `src/snowflake/connector/vendored/requests/exceptions.py:21-22`:
```python
if response is not None and not self.request and hasattr(response, "request"):
    self.request = self.response.request
```
**Critical**: Response objects MUST have a `request` attribute for duck typing compatibility.

#### 3. Test Expectations (MUST be preserved)
From `test/unit/test_network.py:51-57`:
```python
assert exc.value.response.status_code == failed_response.status_code
assert exc.value.response.reason == failed_response.reason
```
From `test/integ/test_put_get_with_gcp_account.py:437`:
```python
exc.response.status_code = 400  # Direct assignment in test mocks
```

#### 4. Storage Client Dependencies (MUST work unchanged)
From `src/snowflake/connector/storage_client.py:64`:
```python
TRANSIENT_ERRORS = (OpenSSL.SSL.SysCallError, Timeout, ConnectionError)
```
- Storage clients catch `ConnectionError` and `Timeout` by type
- They call `response.raise_for_status()` expecting requests behavior

#### 5. Exception Hierarchy Mapping
- `aiohttp.ClientConnectionError` → `requests.ConnectionError(request=None, response=None)`
- `aiohttp.ClientConnectorError` → `requests.ConnectionError(request=None, response=None)`
- `aiohttp.ServerTimeoutError` → `requests.ReadTimeout(request=None, response=None)`
- `aiohttp.ClientTimeout` → `requests.ConnectTimeout(request=None, response=None)`
- `aiohttp.ClientSSLError` → `requests.SSLError(request=None, response=None)`
- `aiohttp.ClientResponseError` → `requests.HTTPError(request=None, response=wrapped_response)`

#### 6. RequestException Constructor Pattern
All requests exceptions inherit from `RequestException` which expects:
```python
def __init__(self, *args, **kwargs):
    response = kwargs.pop("response", None)
    self.response = response
    self.request = kwargs.pop("request", None)
```

#### 7. Generic Error Handling Patterns
From `src/snowflake/connector/network.py:1052` and `1070`:
```python
isinstance(e, requests.exceptions.HTTPError)  # Type checking
isinstance(cause, Error)  # Type checking for Snowflake errors
```

### Implementation Strategy
1. Create sync wrapper classes that provide 100% requests.Session/Response interface
2. Map all aiohttp exceptions to requests exceptions with proper structure
3. Preserve all existing error handling, auth, and parameter patterns
4. Use asyncio.run_coroutine_threadsafe() to convert async calls to sync

## Utility Scripts

- If you need the call graph upstream from a TARGET_FUNCTION (Valid formats include `func`, `class.func`, and `file::class.func`) call `./get_upstream.sh <TARGET_FUNCTION>`. This is useful for determining what depends on TARGET_FUNCTION. Similarly, you can do `./get_downstream.sh <TARGET_FUNCTION>`.
```

## Workflow and Development Guidelines

- **Method Conversion Tracking**
  - Whenever you convert a method from sync to async, record a reference to that method in ASYNC_CONVERSION.md in the project root, following the existing format.