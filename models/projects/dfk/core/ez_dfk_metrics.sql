{{
    config(
        materialized="incremental",
        snowflake_warehouse="DFK",
        database="DFK",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_dfk_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("defi-kingdoms") }})
select
    f.date
    , 'dfk' as artemis_id

    --Market Data
    , price
    , market_cap as mc
    , fdmc
    , token_volume

    --Usage Data  
    , dau as chain_dau
    , dau
    , txns as chain_txns
    , txns
    , fees / txns as chain_avg_txn_fee
    , fees / txns as avg_txn_fee

    --Fee Data
    , fees_native
    , fees_native * price as fees

    --Token Turnover/Other Data
    , token_turnover_circulating
    , token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join price_data on f.date = price_data.date
where true 
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())
