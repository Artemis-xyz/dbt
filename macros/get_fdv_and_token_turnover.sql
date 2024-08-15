{% macro get_fdv_and_token_turnover(coingecko_id) %}

    select
        date as date
        , shifted_token_price_usd as price
        , shifted_token_market_cap as market_cap
        , t2.total_supply * price as fdmc
        , shifted_token_h24_volume_usd / market_cap as token_turnover_circulating
        , shifted_token_h24_volume_usd / fdmc as token_turnover_fdv
        , shifted_token_h24_volume_usd as token_volume
    from {{ ref("fact_coingecko_token_date_adjusted_gold") }} t1
    inner join
        (
            select
                token_id, coalesce(token_max_supply, token_total_supply) as total_supply
            from {{ ref("fact_coingecko_token_realtime_data") }}
            where token_id = '{{ coingecko_id }}'
        ) t2
        on t1.coingecko_id = t2.token_id
    where
        coingecko_id = '{{ coingecko_id }}'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select
        dateadd('day', -1, to_date(sysdate())) as date
        , token_current_price as price
        , token_market_cap as market_cap
        , coalesce(token_max_supply, token_total_supply) * price as fdmc
        , token_total_volume / market_cap as token_turnover_circulating
        , token_total_volume / fdmc as token_turnover_fdv
        , token_total_volume as token_volume    from {{ ref("fact_coingecko_token_realtime_data") }}
    where token_id = '{{ coingecko_id }}'

{% endmacro %}