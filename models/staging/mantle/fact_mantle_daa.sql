-- depends_on: {{ source("PROD_LANDING", "raw_mantle_daa") }}
select date, dau::integer as daa, 'mantle' as chain
from
    (
        {{
            raw_partitioned_array_to_fact_table(
                "landing_database.prod_landing.raw_mantle_daa", "date_time", "DAU"
            )
        }}
    )
