{{ config(materialized="table") }}
with
extracted_raw_data as (
    {{
        unpack_json_array(
            "raw_puffer_finance_restaked_eth_count",
            "source_json",
            column_map=[
                ("date", to_date, "date"),
                ("value", to_float, "total_supply")
            ],
            is_landing_table=true
    )}}
)
select
    date,
    total_supply,
    'ethereum' as chain
from extracted_raw_data