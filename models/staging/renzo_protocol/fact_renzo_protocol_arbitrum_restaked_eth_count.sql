with
extracted_raw_data as (
    {{
        unpack_json_array(
            "raw_renzo_protocol_arbitrum_restaked_eth",
            "source_json",
            column_map=[
                ("date", to_date, "date"),
                ("restaked_eth", to_float, "total_supply")
            ],
            is_landing_table=true
    )}}
)
select
    date,
    total_supply,
    'arbitrum' as chain
from extracted_raw_data
