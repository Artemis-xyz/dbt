{{
    config(
        materialized="table",
        snowflake_warehouse="HYDRATION",
        database="hydration",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}
with
    fundamental_data as (
        select
            date
            , coalesce(txns, 0) as txns
            , coalesce(daa, 0) as dau
            , coalesce(fees_native, 0) as fees_native
            , coalesce(fees_usd, 0) as fees
        from {{ ref("fact_hydration_fundamental_metrics") }}
    )
select
    fundamental_data.date
    , 'hydration' as artemis_id
    , 'hydration' as chain
   
    -- Standardized Metrics

    -- Usage Data
    , fundamental_data.daa as chain_dau
    , fundamental_data.daa as dau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns

    -- Fee Data
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as fees
    
from fundamental_data 
where fundamental_data.date < to_date(sysdate())