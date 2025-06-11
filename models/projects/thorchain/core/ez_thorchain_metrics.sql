{{
    config(
        materialized='table',
        snowflake_warehouse='THORCHAIN',
        database='THORCHAIN',
        schema='core',
        alias='ez_metrics'
    )
}}

with thorchain_tvl as (
    {{ get_defillama_protocol_tvl('thorchain') }}
)

, market_metrics as (
    {{get_coingecko_metrics('thorchain')}}
)

select
    tt.date
    , 'Defillama' as source

    -- Standardized Metrics
    , tt.tvl

    -- Market Metrics
    , mm.price as price
    , mm.token_volume as token_volume
    , mm.market_cap as market_cap
    , mm.fdmc as fdmc
    , mm.token_turnover_circulating as token_turnover_circulating
    , mm.token_turnover_fdv as token_turnover_fdv

from thorchain_tvl tt
left join market_metrics mm using (date)
where tt.date < to_date(sysdate())
and tt.name = 'thorchain' -- macro above returns data for 'Thorchain Lending' too, so we filter by name