{% macro get_wallet_dex_trades(chain) %}
    select
        origin_from_address as address
        , count(distinct tx_hash) as number_dex_trades
        , count(distinct pool_name) as distinct_pools
        , sum(coalesce(amount_out_usd, amount_in_usd)) as total_dex_volume
        , avg(coalesce(amount_out_usd, amount_in_usd)) as avg_dex_trade
        , count(distinct token_out) as distinct_token_out
        , count(distinct token_in) as distinct_token_in
        , max(coalesce(amount_out_usd, amount_in_usd)) as max_dex_trade
        , count(distinct date(block_timestamp)) as distinct_days_traded
        , count(distinct platform) as distinct_dex_platforms
    from {{chain}}_flipside.defi.ez_dex_swaps
    group by origin_from_address
{% endmacro %}
