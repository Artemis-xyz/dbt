create or replace function event_info_to_keccak_event_signature(event variant)
returns string
language python runtime_version = '3.11'
packages = ('pycryptodome==3.15.0')
handler = 'event_info_to_keccak_event_signature'
as $$
from Crypto.Hash import keccak

def event_info_to_keccak_event_signature(event: dict) -> str:
    k = keccak.new(digest_bits=256)
    event_name = event['name']
    event_inputs = [input['type'] for input in event['inputs']]
    event_signature = f"{event_name}({','.join(event_inputs)})"
    k.update(event_signature.encode('utf-8')) 
    return f"0x{k.hexdigest()}"
$$
;
grant usage
on function pc_dbt_db.prod.event_info_to_keccak_event_signature(variant)
to role pc_dbt_role
;
