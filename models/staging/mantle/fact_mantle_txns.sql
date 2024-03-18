-- depends_on: {{ source("PROD_LANDING", "raw_mantle_txns") }}
select date, txns::integer as txns, 'mantle' as chain
from
    (
        {{
            raw_partitioned_array_to_fact_table(
                "landing_database.prod_landing.raw_mantle_txns", "date_time", "TXNS"
            )
        }}
    )
