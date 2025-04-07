{{
    config(
        materialized='table',
        snowflake_warehouse='ENZYME',
        database='ENZYME',
        schema='core',
        alias='ez_metrics'
    )
}}

with dim_date_spine as (
    select 
        date,
    from {{ ref('dim_date_spine') }}
    where date between '2017-02-21' and to_date(sysdate())
)
, token_holders as (
    select
        date,
        token_holder_count
    from {{ ref('fact_enzyme_token_holders') }}
)
, market_data as (
    {{ get_coingecko_metrics('melon') }}
)

select
    ds.date
    , th.token_holder_count

    -- Token Metrics
    , md.price
    , md.market_cap
    , md.fdmc
    , md.token_volume

    -- Turnover Metrics
    , md.token_turnover_circulating
    , md.token_turnover_fdv
from dim_date_spine ds
left join token_holders th using (date)
left join market_data md using (date)