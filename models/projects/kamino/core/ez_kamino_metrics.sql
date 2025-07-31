{{
    config(
        materialized="incremental",
        snowflake_warehouse="KAMINO",
        database="kamino",
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
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine")}}
        WHERE date >= '2023-11-01' AND date < to_date(sysdate())
    )
    
    , kamino_tvl AS (
        SELECT 
            date, 
            sum(usd_balance) as tvl
        FROM {{ ref("fact_kamino_tvl") }}
        GROUP BY date
    )
    , klend_fees_and_revenue AS (
       SELECT 
            date, 
            klend_fees_usd as fees, 
            klend_revenue_usd as revenue
        FROM {{ ref("fact_kamino_fees_and_revenues") }}
    )
    , kamino_transactions AS (    
        SELECT 
            date, 
            tx_count as txn, 
            dau
        FROM {{ ref("dim_kamino_transactions") }}
    )

    , market_data AS (
        {{ get_coingecko_metrics('kamino') }}
    )

    SELECT
        date_spine.date
        , 'kamino' as artemis_id

        -- Market Data
        , market_data.price
        , market_data.market_cap
        , market_data.fdmc
        , market_data.token_volume

        -- Usage Data
        , dau AS lending_dau
        , dau
        , txn AS lending_txns
        , txn AS txns
        , tvl AS lending_deposits
        , tvl 

        -- Fee Data
        , fees AS lending_fees
        , fees

        -- Financial Statements
        , revenue

        -- Turnover Data
        , market_data.token_turnover_circulating
        , market_data.token_turnover_fdv

        -- Timestamp Columns
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
    FROM date_spine
    LEFT JOIN kamino_tvl USING (date)
    LEFT JOIN klend_fees_and_revenue USING (date)
    LEFT JOIN market_data USING (date)
    LEFT JOIN kamino_transactions USING (date)
    WHERE true
    {{ ez_metrics_incremental('date_spine.date', backfill_date) }}
    AND date_spine.date < to_date(sysdate())
