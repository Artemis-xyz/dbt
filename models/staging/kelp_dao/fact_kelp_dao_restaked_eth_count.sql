-- depends_on: {{ source("PROD_LANDING", "raw_kelp_dao_restaked_eth_count") }}
{{ config(materialized="table") }}
with
extracted_raw_data as (
    {{raw_partitioned_array_to_fact_table_many_columns(
        "landing_database.prod_landing.raw_kelp_dao_restaked_eth_count",
        "date",
        ("address", "total_asset_deposits")
    )}}
)
select
    date,
    sum(total_asset_deposits::float) as total_supply,
    'ethereum' as chain
from extracted_raw_data
group by 1
