{{
    config(
        materialized="table"
        , snowflake_warehouse="PARALLEL_FINANCE"
        , database="parallel_finance"
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
        from {{ ref("fact_parallel_finance_daa_gas_gas_usd_txns") }}
    )
    , defillama_data as ({{ get_defillama_metrics("parallel") }})

select
    fundamental_data.date
    , 'parallel_finance' as chain
    , txns
    , dau
    , fees
    , fees_native
    , tvl
from fundamental_data
left join defillama_data using (date)
where fundamental_data.date < to_date(sysdate())