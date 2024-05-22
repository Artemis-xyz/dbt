
with
extracted_raw_data as (
    {{
        unpack_json_array(
            "raw_renzo_protocol_ethereum_restaked_eth",
            "source_json",
            column_map=[
                ("date", to_date, "date"),
                ("l1_restaked_eth", to_float, "l1_restaked_eth"),
                ("bridged_restaked_eth", to_float, "bridged_restaked_eth")
            ],
            is_landing_table=true
    )}}
)
select
    date,
    l1_restaked_eth,
    bridged_restaked_eth,
    l1_restaked_eth - bridged_restaked_eth as total_supply,
    'ethereum' as chain
from extracted_raw_data
