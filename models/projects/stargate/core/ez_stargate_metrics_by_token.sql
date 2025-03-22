
{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

with
treasury_models as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_stargate_v2_arbitrum_treasury_balance"),
                ref("fact_stargate_v2_avalanche_treasury_balance"),
                ref("fact_stargate_v2_base_treasury_balance"),
                ref("fact_stargate_v2_bsc_treasury_balance"),
                ref("fact_stargate_v2_ethereum_treasury_balance"),
                ref("fact_stargate_v2_optimism_treasury_balance"),
                ref("fact_stargate_v2_polygon_treasury_balance"),
                ref("fact_stargate_v2_mantle_treasury_balance"),
            ],
        )
    }}
)
, treasury_metrics as (
    select
        date
        , case when lower(symbol) = 'weth' then 'ETH' else upper(symbol) end as token
        , sum(balance_adjusted) as treasury_native
        , sum(balance_usd) as treasury_usd
        , sum(balance_native) as treasury_raw
    from treasury_models
    left join {{ ref("dim_coingecko_token_map")}} using (contract_address, chain)
    where round(balance_usd, 2) > 0
    group by date, token
)

select 
    date
    , token
    --Standardized Metrics
    , treasury_usd as treasury
    , treasury_native
from treasury_metrics
where treasury_usd > 1000
and date < to_date(sysdate())
