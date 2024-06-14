-- depends_on: {{ source("PROD_LANDING", "raw_mantle_txns") }}
{{ config(materialized="view", snowflake_warehouse="MANTLE") }}

select date, txns::integer as txns, 'mantle' as chain
from
    (
        {{
            raw_partitioned_array_to_fact_table(
                "landing_database.prod_landing.raw_mantle_txns", "date_time", "TXNS"
            )
        }}
    )
