
{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_current_positions",
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


select 
    protocol
    , upper(symbol) as symbol
    , sum(balance_native) as balance_native
    , sum(balance) as balance
from treasury_models
where date = (select max(date) from treasury_models)
    and balance > 2 and balance is not null
group by protocol, upper(symbol)
