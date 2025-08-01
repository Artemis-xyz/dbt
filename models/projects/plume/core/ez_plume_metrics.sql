{{
    config(
        materialized="incremental",
        snowflake_warehouse="PLUME",
        database="plume",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
    , rwa_tvl
    , stablecoin_total_supply
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from {{ ref("fact_plume_fundamental_metrics") }}
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
