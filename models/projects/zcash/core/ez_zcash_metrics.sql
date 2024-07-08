{{
    config(
        materialized="table",
        snowflake_warehouse="ZCASH",
        database="zcash",
        schema="core",
        alias="ez_metrics"
    )
}}

with
    fundamental_data as (
        select
            date
            , txns
            , gas_usd as fees
            , gas as fees_native
        from {{ ref("fact_zcash_gas_gas_usd_txns") }}
    )
    , github_data as ({{ get_github_metrics("zcash") }})

select 
    fundamental_data.date
    , 'zcash' as chain
    , txns
    , fees
    , fees_native
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
from fundamental_data
left join github_data using (date)
where fundamental_data.date < to_date(sysdate())
