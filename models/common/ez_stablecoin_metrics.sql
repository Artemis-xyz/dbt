{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
    )
}}

select 
    date,
    total_supply,
    txns,
    dau,
    transfer_volume,
    chain,
    symbol,
    contract_address
from {{ source("PC_DBT_DB_UPSTREAM", "agg_daily_stablecoin_metrics") }} as agg_daily_stablecoin_metrics