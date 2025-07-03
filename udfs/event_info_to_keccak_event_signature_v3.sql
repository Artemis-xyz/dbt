create or replace function event_info_to_keccak_event_signature_v3(event variant)
returns string
language python runtime_version = '3.11'

handler='compute'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = (
    'eth-utils'
    , 'pycryptodome'
)

as $$
from eth_utils import keccak, to_hex

def get_type_string(param):
    """
    Recursively build the type string for a parameter, expanding tuples inline.
    """
    if param['type'].startswith('tuple'):
        # Recursively get types for components
        components = param.get('components', [])
        inner_types = ','.join(get_type_string(c) for c in components)
        # Handle array suffix if present
        array_suffix = param['type'][5:]  # e.g., '[]' or '[2]' or ''
        return f'({inner_types}){array_suffix}'
    else:
        return param['type']

def compute(event):
    """
    Calculate the topic zero (event signature hash) for an event ABI.
    
    Args:
        event_abi (dict): The event ABI dictionary
        
    Returns:
        str: The topic zero hash (0x prefixed hex string)
    """
    name = event['name']
    inputs = event['inputs']
    param_types = [get_type_string(input_param) for input_param in inputs]
    signature = f"{name}({','.join(param_types)})"
    signature_bytes = signature.encode('utf-8')
    topic_zero = keccak(signature_bytes)
    return to_hex(topic_zero)
$$
;
grant usage
on function pc_dbt_db.prod.event_info_to_keccak_event_signature_v3(variant)
to role pc_dbt_role
;