#!/usr/bin/env python3
"""
Utility to convert PEM private key to raw format for Snowflake connector.
"""

import base64
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

def convert_pem_to_raw(pem_private_key: str, passphrase: str = None) -> str:
    """Convert PEM private key to raw format for Snowflake connector.
    
    Args:
        pem_private_key: PEM formatted private key string
        passphrase: Optional passphrase for encrypted keys
        
    Returns:
        Base64 encoded raw private key
    """
    # Load the PEM private key
    private_key_bytes = serialization.load_pem_private_key(
        pem_private_key.encode(),
        backend=default_backend(),
        password=passphrase.encode() if passphrase else None,
    )
    
    # Convert to DER format (raw bytes)
    private_key_der = private_key_bytes.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    
    # Base64 encode for storage
    return base64.b64encode(private_key_der).decode('utf-8')

def main():
    """Convert PEM key from .env to raw format."""
    load_dotenv()
    
    pem_key = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEYPHRASE')
    
    if not pem_key:
        print("❌ SNOWFLAKE_PRIVATE_KEY not found in .env file")
        return
    
    try:
        raw_key = convert_pem_to_raw(pem_key, passphrase)
        print("✅ PEM key converted to raw format:")
        print(f"SNOWFLAKE_PRIVATE_KEY_RAW={raw_key}")
        print("\nAdd this to your .env file to use raw format authentication.")
        
    except Exception as e:
        print(f"❌ Error converting key: {e}")

if __name__ == "__main__":
    main()