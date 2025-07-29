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
    , 'abstract' as artemis_id

    -- Standardized Metrics

    -- Usage Metrics
    , f.txns as chain_txns
    , f.txns as txns
    , f.daa as chain_dau
    , f.daa as dau

    -- Cash Flow Metrics
    , f.fees
    , f.fees_native
    , f.cost as l1_fee_allocation
    , f.cost_native as l1_fee_allocation_native
    , f.revenue as foundation_fee_allocation
    , f.revenue_native as foundation_fee_allocation_native

    -- Financial Metrics
    , f.revenue
    , f.revenue_native

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_abstract_fundamental_metrics") }} as f
where true
{{ ez_metrics_incremental("f.date", backfill_date) }}
and f.date  < to_date(sysdate())

