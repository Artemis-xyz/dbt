{{ config(materialized="table", snowflake_warehouse="OPTIMISM") }}

with wallet_dex_data as ({{ get_wallet_dex_trades("optimism") }})
select
    address,
    number_dex_trades,
    distinct_pools,
    total_dex_volume,
    avg_dex_trade,
    distinct_dex_platforms,
    distinct_token_out,
    distinct_token_in,
    max_dex_trade,
    distinct_days_traded
from wallet_dex_data
