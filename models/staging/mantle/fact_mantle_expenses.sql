-- depends_on: {{ source("PROD_LANDING", "raw_mantle_expenses") }}
{{ config(materialized="view", snowflake_warehouse="MANTLE") }}
select
    date,
    case
        when try_cast(expenses::string as float) is not null
        then try_cast(expenses::string as float) / 1e18
        else 0
    end as expenses,
    'mantle' as chain
from
    (
        {{
            raw_partitioned_array_to_fact_table(
                "landing_database.prod_landing.raw_mantle_expenses",
                "date_time",
                "EXPENSES",
            )
        }}
    )
