{{
    config(
        materialized="table",
        snowflake_warehouse="SCROLL",
        database="scroll",
        schema="core",
        alias="ez_metrics",
    )
}}


with
    fundamental_data as (
        select
            date
            , txns
            , daa as dau
            , gas_usd as fees
            , gas as fees_native
            , revenue
            , revenue_native
            , l1_data_cost
            , l1_data_cost_native
        from {{ ref("fact_scroll_txns") }}
        left join {{ ref("fact_scroll_daa") }} using (date)
        left join {{ ref("fact_scroll_gas_gas_usd_revenue") }} using (date)
    )
    , github_data as ({{ get_github_metrics("scroll") }})
    , contract_data as ({{ get_contract_metrics("scroll") }})
    , defillama_data as ({{ get_defillama_metrics("scroll") }})

select
    fundamental_data.date
    , 'scroll' as chain
    , txns
    , dau
    , fees
    , fees_native
    , revenue
    , revenue_native
    , l1_data_cost
    , l1_data_cost_native
    , tvl
    , dex_volumes
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
from fundamental_data
left join github_data using (date)
left join contract_data using (date)
left join defillama_data using (date)
where fundamental_data.date < to_date(sysdate())