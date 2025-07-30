{{
    config(
        materialized="incremental",
        snowflake_warehouse="BIFROST",
        database="bifrost",
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
            date, 
            txns,
            daa, 
            fees_native, 
            fees_usd
        from {{ ref("fact_bifrost_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics('bifrost-native-coin') }})
select
    f.date
    , 'bitfrost' as artemis_id

    --Market Data
    , price_data.price
    , price_data.market_cap as mc
    , price_data.fdmc
    , price_data.token_volume

    --Usage Data
    , daa as chain_dau
    , daa as dau
    , txns as chain_txns
    , txns

    --Fee Data
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees

    --Token Turnover/Other Data
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join price_data using(f.date)
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())
