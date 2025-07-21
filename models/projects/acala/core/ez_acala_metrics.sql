--depends_on: {{ ref("fact_acala_rolling_active_addresses") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="ACALA",
        database="acala",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        -- merge_update_columns=["modified_on", <new_columns>], -- can specify specific columns to backfill but going to be manual work
        full_refresh=false
    )
}}

{% set backfill_date = None %}

with
    fundamental_data as (
        select
            date, chain, daa, txns, fees_native, fees_usd as fees, fees_native * .2 as revenue_native, fees_usd * .2 as revenue
        from {{ ref("fact_acala_fundamental_metrics") }}
        {% if is_incremental() %}
            {% if backfill_date %}
                where date >= '{{ backfill_date }}'
            {% else %}
                where date > (select max(this.date) from {{ this }} as this)
            {% endif %}
        {% endif %}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("acala") }}),
    price_data as ({{ get_coingecko_metrics("acala") }})
select
    fundamental_data.date
    , fundamental_data.chain
    , daa as dau
    , txns
    , fees_native
    , fees
    , fees / txns as avg_txn_fee
    , revenue
    , wau
    , mau
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    -- Cashflow metrics
    , fees as chain_fees
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , revenue_native AS burned_fee_allocation_native
    , revenue AS burned_fee_allocation
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join price_data on fundamental_data.date = price_data.date
where fundamental_data.date < to_date(sysdate())
{% if is_incremental() %}
    {% if backfill_date %}
        and date >= '{{ backfill_date }}'
    {% else %}
        and date > (select max(this.date) from {{ this }} as this)
    {% endif %}
{% endif %}
