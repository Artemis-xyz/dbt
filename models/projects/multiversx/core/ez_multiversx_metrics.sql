{{
    config(
        materialized="incremental"
        , snowflake_warehouse="MULTIVERSX"
        , database="multiversx"
        , schema="core"
        , alias="ez_metrics"
        , incremental_strategy="merge"
        , unique_key="date"
        , on_schema_change="append_new_columns"
        , merge_update_columns=var("backfill_columns", [])
        , merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list
        , full_refresh=false
        , tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select
            date
            , txns
            , daa::number as dau
            , gas as fees_native
            , gas_usd as fees
            , case when txns > 0 then fees / txns end as avg_txn_fee
        from {{ ref("fact_multiversx_txns") }}
        left join {{ ref("fact_multiversx_daa") }} using (date)
        left join {{ ref("fact_multiversx_gas_gas_usd") }} using (date)
    )
    , github_data as ({{ get_github_metrics("elrond") }})
    , defillama_data as ({{ get_defillama_metrics("elrond") }})
    , price_data as ({{ get_coingecko_metrics("elrond-erd-2") }})

select
    f.date

    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , f.txns as chain_txns
    , f.txns
    , f.dau as chain_dau
    , f.dau
    , f.avg_txn_fee as chain_avg_txn_fee
    , dfl.dex_volumes as chain_spot_volume
    , dfl.tvl

    -- Fees Metrics
    , f.fees
    , f.fees_native

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- Other Metrics
    , token_turnover_circulating
    , token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join github_data using (f.date)
left join defillama_data dfl using (f.date)
left join price_data using (f.date)
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())
