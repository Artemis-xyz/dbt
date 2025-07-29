{{
    config(
        materialized="incremental",
        snowflake_warehouse="EULER",
        database="euler",
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

with lending_metrics as (
    select
        date
        , sum(supplied_amount_cumulative) as lending_deposits
        , sum(borrow_amount_cumulative) as lending_loans
        , sum(supplied_amount_cumulative - borrow_amount_cumulative) as tvl
    from {{ ref("fact_euler_borrow_and_lending_metrics_by_chain") }}
    group by 1
)
, market_metrics as (
    {{get_coingecko_metrics("euler")}}
)
, date_spine as (
    SELECT
        date
    from {{ ref("dim_date_spine") }}
    where date between (select min(date) from lending_metrics) and to_date(sysdate())
)
select
    ds.date
    , coalesce(lending_deposits, 0) as lending_deposits
    , coalesce(lending_loans, 0) as lending_loans
    , coalesce(tvl, 0) as tvl
    , coalesce(market_metrics.price, 0) as price
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine ds
left join lending_metrics using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())