create or replace function event_info_to_keccak_event_signature_v2(event variant)
returns string
language python runtime_version = '3.11'
packages = ('pycryptodome==3.15.0')
handler = 'event_info_to_keccak_event_signature_v2'
as $$
from Crypto.Hash import keccak

def event_info_to_keccak_event_signature_v2(event: dict) -> str:
    k = keccak.new(digest_bits=256)
    event_name = event['name']
    input_types = []
    for input in event['inputs']:
        if 'components' in input:
            comp_array = []
            for component in input['components']:
                comp_array.append(component['type'])
            input_types.append(comp_array)
        else:
            input_types.append([input['type']])
    input_string = ''
    count = 0
    for input_type in input_types:
        if len(input_type) > 1:
            input_string += f"({','.join(input_type)})"
            if count < len(input_types) - 1:
                input_string += ','
            count += 1
        else:
            input_string += input_type[0]
            if count < len(input_types) - 1:
                input_string += ','
        count += 1
    event_signature = f"{event_name}({input_string})"
    k.update(event_signature.encode('utf-8')) 
    return f"0x{k.hexdigest()}"
$$
;
grant usage
on function pc_dbt_db.prod.event_info_to_keccak_event_signature_v2(variant)
to role pc_dbt_role
;