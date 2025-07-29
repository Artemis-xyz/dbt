{{
    config(
        materialized="incremental",
        snowflake_warehouse="ABSTRACT",
        database="abstract",
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

select
    f.date
    , txns
    , daa as dau
    , fees_native
    , fees
    , cost
    , cost_native
    , revenue
    , revenue_native

    -- Standardized Metrics

    -- Chain Usage Metrics
    , txns as chain_txns
    , daa as chain_dau

    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , cost as l1_fee_allocation
    , cost_native as l1_fee_allocation_native
    , revenue as foundation_fee_allocation
    , revenue_native as foundation_fee_allocation_native

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_abstract_fundamental_metrics") }} as f
where true
{{ ez_metrics_incremental("f.date", backfill_date) }}
and f.date  < to_date(sysdate())

