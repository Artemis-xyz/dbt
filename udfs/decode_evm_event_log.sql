create or replace function decode_evm_event_log(log_data varchar, event_abi variant)
returns variant
language python runtime_version = '3.11'
handler = 'decode_evm_event_log'
as
    $$
def decode_evm_event_log(log_data:str, event_abi:dict) -> dict:
    """
    This function currently has support to decode: bool, address, uint<N>.
    Future Plans: Future plans change this implementation to use the web3 library to decode the event logs.
        This library relies on OS specific code and requires snowflake to support it on their end. This is
        suppose to take 5-6 weeks from 03-26-2024
    """
    acceptable_types = ['bool', 'address']

    def big_endian_to_int(value: bytes) -> int:
        return int.from_bytes(value, "big")

    def validate_types(types):
        for input_type in types:
            if input_type not in acceptable_types and 'uint' not in input_type:
                raise Exception(f"Invalid event type, decode function does not support {input_type} decode functionality")

    input_types = [input['type'] for input in event_abi['inputs']]
    input_names = [input['name'] for input in event_abi['inputs']]
    validate_types(input_types)
    chunk_size = 64
    byte_array = [log_data[i:i+chunk_size] for i in range(0, len(log_data), chunk_size)]

    decoded = []
    for idx, byte_entry in enumerate(byte_array):
        if(input_types[idx] == 'address'):
            decoded.append('0x' + byte_entry[24:])
        elif 'uint' in input_types[idx] or input_types[idx] == 'bool':
            decoded.append(big_endian_to_int(bytearray.fromhex(byte_entry)))
            
    decoded = [str(value) for value in decoded]
    decoded = dict(zip(input_names, decoded))
    return decoded
    
$$
;
grant usage
on function pc_dbt_db.prod.decode_evm_event_log(varchar, variant)
to role pc_dbt_role
;
