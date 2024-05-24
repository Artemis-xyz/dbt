-- depends_on: {{ source("PROD_LANDING", "raw_eigenpie_restaked_eth_count") }}
{{ config(materialized="table") }}
with
extracted_raw_data as (
    {{raw_partitioned_array_to_fact_table_many_columns(
        "landing_database.prod_landing.raw_eigenpie_restaked_eth_count",
        "date",
        ("mLRT_address", "total_supply")
    )}}
)
select
    date,
    sum(total_supply::float) as total_supply,
    'ethereum' as chain
from extracted_raw_data
group by 1