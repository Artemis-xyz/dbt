{{
    config(
        materialized='incremental',
        snowflake_warehouse='ENZYME',
        database='ENZYME',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with dim_date_spine as (
    select 
        date,
    from {{ ref('dim_date_spine') }}
    where date between '2017-02-21' and to_date(sysdate())
)
, token_holders as (
    select
        date,
        token_holder_count
    from {{ ref('fact_enzyme_token_holders') }}
)
, market_data as (
    {{ get_coingecko_metrics('melon') }}
)

select
    ds.date
    , 'enzyme' AS artemis_id

    -- Standardized Metrics

    -- Token Data
    , md.price
    , md.market_cap
    , md.fdmc
    , md.token_volume

    -- Usage Data
    , th.token_holder_count

    -- Turnover Metrics
    , md.token_turnover_circulating
    , md.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from dim_date_spine ds
left join token_holders th using (date)
left join market_data md using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())