
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
    from treasury_models
    inner join {{ ref("dim_coingecko_token_map")}} using (contract_address, chain)
    where round(balance_usd, 2) > 0
    group by date, symbol
)

select 
    date
    , token
    --TODO the endpoint StargateTreasuryTokensView relies on the treasury_usd column we will need to change the endpoint once we port of snowflake
    , treasury_usd
    , treasury_native
from treasury_metrics
where date < to_date(sysdate()) and treasury_usd > 1000
