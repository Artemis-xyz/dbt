{{
    config(
        materialized='incremental',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='core',
        alias='ez_metrics',
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

with swap_metrics as (
    SELECT
        block_timestamp::date as date,
        COUNT(DISTINCT sender) as unique_traders,
        COUNT(*) as total_swaps,
        SUM(amount_in_usd) as daily_volume_usd,
        SUM(fee_usd) as daily_fees_usd
    FROM {{ ref('fact_aerodrome_swaps') }}
    GROUP BY 1
)
, tvl_metrics as (
    SELECT
        date,
        SUM(token_balance_usd) as tvl_usd
    FROM {{ ref('fact_aerodrome_tvl') }}
    GROUP BY date
)
, market_data as (
    {{get_coingecko_metrics('aerodrome-finance')}}
)
, supply_metrics as (
    SELECT
        date,
        pre_mine_unlocks,
        emissions_native,
        locked_supply,
        total_supply,
        circulating_supply_native, 
        buybacks_native, 
        buybacks
    FROM {{ ref('fact_aerodrome_supply_data') }}
)
, pools_metrics as (
    SELECT
        date,
        cumulative_count
    FROM {{ ref('fact_aerodrome_pools') }}
)
, token_incentives as (
    select
        day as date,
        usd_value as token_incentives
    from {{ref('fact_aerodrome_token_incentives')}}
)
, date_spine as (
    SELECT
        ds.date
    FROM {{ ref('dim_date_spine') }} ds
    WHERE ds.date >= (
        select MIN(
            {% if backfill_date %}
                '{{ backfill_date }}'
            {% else %}
                (SELECT MAX(this.date) FROM {{ this }} as this)
            {% endif %}
        )
    )
        and ds.date < to_date(sysdate())
)

SELECT
    ds.date
    , 'aerodrome' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , md.price
    , md.market_cap
    , md.fdmc
    , md.token_volume

    -- Usage Data
    , sm.unique_traders as spot_dau
    , sm.unique_traders as dau
    , sm.total_swaps as spot_txns
    , sm.total_swaps as txns
    , tm.tvl_usd as spot_tvl
    , tm.tvl_usd as tvl
    , sm.daily_volume_usd as spot_volume

    -- Cash Flow Metrics
    , sm.daily_fees_usd as fees
    , sm.daily_fees_usd as spot_fees
    , sm.daily_fees_usd as staking_fee_allocation
    , sp.buybacks AS buybacks

    -- Financial Statements
    , (COALESCE(sm.daily_fees_usd, 0) + COALESCE(sp.buybacks, 0)) as revenue
    , ti.token_incentives
    , (COALESCE(sp.buybacks, 0)) - COALESCE(ti.token_incentives, 0) as earnings
    -- NOTE: We do not track bribes as a part of revenue here. 

    -- Supply Metrics
    , COALESCE(sp.emissions_native, 0) AS gross_emissions_native
    , COALESCE(sp.emissions_native, 0) * COALESCE(md.price, 0) AS gross_emissions
    , sp.total_supply AS total_supply
    , COALESCE(sp.pre_mine_unlocks, 0) AS premine_unlocks_native
    , sp.circulating_supply_native AS circulating_supply_native

    -- Turnover Data
    , md.token_turnover_circulating
    , md.token_turnover_fdv

    -- Bespoke Metrics
    , pm.cumulative_count as total_pools

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine ds
LEFT JOIN swap_metrics sm using (date)
LEFT JOIN tvl_metrics tm using (date)
LEFT JOIN market_data md using (date)
LEFT JOIN supply_metrics sp using (date)
LEFT JOIN pools_metrics pm using (date)
LEFT JOIN token_incentives ti using (date)
WHERE true 
{{ ez_metrics_incremental("ds.date", backfill_date) }}
and ds.date < to_date(sysdate())