{{
    config(
        materialized="table"
        , snowflake_warehouse="ZORA"
        , database="zora"
        , schema="core"
        , alias="ez_metrics"
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
        from {{ ref("fact_zora_txns") }}
        left join {{ ref("fact_zora_daa") }} using (date)
        left join {{ ref("fact_zora_gas_gas_usd_revenue") }} using (date)
    )
    , github_data as ({{ get_github_metrics("zora") }})
    , contract_data as ({{ get_contract_metrics("zora") }})
    , defillama_data as ({{ get_defillama_metrics("zora") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("zora") }})

select
    fundamental_data.date
    , 'zora' as chain
    , txns
    , dau
    , wau
    , mau
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
left join rolling_metrics using (date)
where fundamental_data.date < to_date(sysdate())