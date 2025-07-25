{{
    config(
        materialized = "incremental",
        snowflake_warehouse = "CHILIZ",
        database = "CHILIZ",
        schema = "core",
        alias = "ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with date_spine as(
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date between '2018-10-26' and to_date(sysdate())
)
, fees as (
    select
        date,
        fees_native,
        fees
    from {{ref("fact_chiliz_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_chiliz_txns")}}
),
daus as (
    select
        date,
        dau
    from {{ref("fact_chiliz_dau")}}
    where dau < 170000 -- There is a DQ issue with the Chiliz dau data: 2 days with > 170k DAU while the rest of the data around those days is < 1k
),
burns as (
    select
        date,
        burns_native,
        revenue
    from {{ref("fact_chiliz_burns")}}
),
treasury as (
    select
        date,
        native_balance,
        native_balance_change,
        usd_balance,
        usd_balance_change
    from {{ref("fact_chiliz_treasury")}}
),
price_data as ({{ get_coingecko_metrics("chiliz") }}),
supply_data as (
    select
        date,
        gross_emissions_native,
        circulating_supply_native
    from {{ref("fact_chiliz_supply")}}
)
select
    ds.date
    , 'chiliz' as artemis_id
    
    -- Standardized Metrics
    
    -- Market Data 
    , price
    , market_cap
    , fdmc
    , token_volume
    
    -- Usage Data
    , dau AS chain_dau
    , dau
    , txns AS chain_txns
    , txns 
    
    -- Fee Data
    , fees_native
    , fees
    , fees AS chain_fees
    , revenue AS burned_fee_allocation

    -- Financial Statements
    , revenue 
    
    -- Treasury Data
    , usd_balance AS treasury

    -- Supply metrics
    , gross_emissions_native
    , gross_emissions_native * price AS gross_emissions 
    , circulating_supply_native
    , burns_native 

    -- Turnover Data
    , token_turnover_circulating
    , token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine ds
left join fees using (date)
left join txns using (date)
left join daus using (date)
left join burns using (date)
left join treasury using (date)
left join price_data using (date)
left join supply_data using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())