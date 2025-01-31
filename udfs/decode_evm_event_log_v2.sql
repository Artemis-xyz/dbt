create or replace function decode_evm_event_log_v2(tx_hash varchar, log_data varchar, event_abi variant)
returns variant
language python runtime_version = '3.11'
handler = 'decode_evm_event_log_v2'
as
    $$
def decode_evm_event_log_v2(topic_data:str, log_data:str, parse_json) -> dict:
    """
    This function currently has support to decode: bool, address, uint<N>.
    Future Plans: Future plans change this implementation to use the web3 library to decode the event logs.
        This library relies on OS specific code and requires snowflake to support it on their end. This is
        suppose to take 5-6 weeks from 03-26-2024
    """
    acceptable_types = ['bool', 'address']

    def big_endian_to_int(value: bytes) -> int:
        return int.from_bytes(value, "big")

    chunk_size = 64
    byte_array = [log_data[i:i+chunk_size] for i in range(0, len(log_data), chunk_size)]
    topic_byte_array = [topic_data[i:i+chunk_size] for i in range(0, len(topic_data), chunk_size)]
    complete_byte_array = []
    input_types = []
    input_names = []
    if type(parse_json) != dict:
        return None

    for input in parse_json['inputs']:
        if input['type'] in acceptable_types or 'uint' in input['type']:
            input_types.append(input['type'])
            input_names.append(input['name'])
            if 'indexed' in input and input['indexed'] and len(topic_byte_array) > 0:
                complete_byte_array.append(topic_byte_array.pop(0))
            if len(byte_array) > 0 and input['indexed'] == False:
                complete_byte_array.append(byte_array.pop(0))

    decoded = []
    for idx, byte_entry in enumerate(complete_byte_array):
        try:
            if(input_types[idx] == 'address'):
                decoded.append('0x' + byte_entry[24:])
            elif 'uint' in input_types[idx] or input_types[idx] == 'bool':
                decoded.append(big_endian_to_int(bytearray.fromhex(byte_entry)))
        except Exception as e:
            continue
            
    decoded = [str(value) for value in decoded]
    decoded = dict(zip(input_names, decoded))
    return decoded
    
$$
;
grant usage
on function pc_dbt_db.prod.decode_evm_event_log_v2(varchar, varchar, variant)
to role pc_dbt_role
;
