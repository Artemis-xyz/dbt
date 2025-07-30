
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
        , sum(balance_native) as treasury_native
        , sum(balance) as treasury
    from treasury_models
    left join {{ ref("dim_coingecko_token_map")}} using (contract_address, chain)
    where balance > 2 and balance is not null
    group by date, token
)

select 
    treasury_metrics.date
    , 'stargate' as artemis_id
    , treasury_metrics.token

    --Standardized Metrics

    -- Treasury Data
    , treasury_metrics.treasury_native as treasury
    , treasury_metrics.treasury
    
from treasury_metrics
where treasury > 1000
and date < to_date(sysdate())
