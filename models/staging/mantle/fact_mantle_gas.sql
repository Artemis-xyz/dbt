-- depends_on: {{ source("PROD_LANDING", "raw_mantle_gas") }}
{{ config(materialized="view", snowflake_warehouse="MANTLE") }}

select date, gas::float / 1e18 as gas, 'mantle' as chain
from
    (
        {{
            raw_partitioned_array_to_fact_table(
                "landing_database.prod_landing.raw_mantle_gas", "date_time", "GAS"
            )
        }}
    )
