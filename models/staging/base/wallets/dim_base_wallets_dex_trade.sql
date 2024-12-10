{{ config(materialized="table", snowflake_warehouse="BASE_MD") }}

with wallet_dex_data as ({{ get_wallet_dex_trades("base") }})
select
    address,
    number_dex_trades,
    distinct_pools,
    total_dex_volume,
    avg_dex_trade,
    distinct_dex_platforms,
    distint_token_out,
    distinct_token_in,
    max_dex_trade,
    distinct_days_traded
from wallet_dex_data
