{{
    config(
        materialized="incremental",
        snowflake_warehouse="BITPAY",
        database="bitpay",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
    )
}}

select
    date::date as date,
    sum(transfer_volume) as transfer_volume

    -- Timetamp Columns
    , sysdate() as created_on
    , sysdate() as modified_on
from {{ ref("fact_bitpay_transfers") }}
WHERE TRUE
{{ ez_metrics_incremental('date', backfill_date) }}
group by 1