{{config(snowflake_warehouse="INJECTIVE")}}

select date, fees_native_all as fees_native, chain
from {{ source("PROD", "fact_injective_fees_native_all_silver") }}
where coingecko_id = 'injective-protocol'
