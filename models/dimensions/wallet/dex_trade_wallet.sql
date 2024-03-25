select
    origin_from_address,
    count(*) number_dex_trades,
    count(distinct pool_name) distinct_pools,
    sum(amount_in_usd) total_dex_volume,
    avg(amount_in_usd) avg_dex_trade,
    count(distinct platform) distinct_dex_platforms,
    count(distinct token_out) distint_token_out,
    count(distinct token_in) distinct_token_in,
    max(amount_in_usd) max_dex_trade,
    count(distinct date(block_timestamp)) distinct_days_traded
from flipside_arbitrum.silver_dex.complete_dex_swaps
group by 1
