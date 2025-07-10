create or replace function decode_sei_deterministic_address_to_evm(sei_address string)
returns string
language PYTHON
runtime_version = '3.9'
handler = 'compute'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = (
    'bech32'
)
AS $$

import bech32

def compute(sei_address):
    try:
        hrp, data = bech32.bech32_decode(sei_address)
        decoded = bech32.convertbits(data, 5, 8, False)
        pubkey_hash = bytes(decoded)
        result = '0x' + pubkey_hash.hex()
        return result
    except Exception as e:
        return f"error: {type(e).__name__.lower()} - {str(e)[:200]}"
$$;