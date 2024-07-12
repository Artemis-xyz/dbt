{% macro chainlink_vrf_rewards_daily(chain) %}
with
    logs as (
        select 
            *
        from {{ ref("fact_chainlink_"~chain~"_vrf_request_fulfilled_logs")}}
    )
    , vrf_daily as (
        SELECT
            cast(date_trunc('day', evt_block_time) AS date) AS date_start,
            SUM(token_value) as token_amount
        FROM logs 
        GROUP BY 1
    )
    , link_usd_daily AS ({{get_coingecko_price_with_latest("chainlink")}})
    , vrf_reward_daily AS (
        SELECT
            vrf_daily.date_start,
            COALESCE(vrf_daily.token_amount, 0) as token_amount,
            COALESCE(vrf_daily.token_amount * lud.price, 0)  as usd_amount
        FROM vrf_daily
        LEFT JOIN link_usd_daily lud ON lud.date = vrf_daily.date_start
    )
    SELECT
        '{{chain}}' as blockchain
        , date_start as date
        , token_amount
        , usd_amount
    from vrf_reward_daily
{% endmacro %}