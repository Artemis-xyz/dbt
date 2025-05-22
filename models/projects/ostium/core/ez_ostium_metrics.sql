{{
    config(
        materialized="view",
        database="ostium",
        snowflake_warehouse="OSTIUM",
        schema="core",
        alias="ez_metrics"
    )
}}

select
    date

    --Standardized Metrics

    --Usage Metrics
    , coalesce(trades, 0) as perp_txns
    , coalesce(traders, 0) as perp_dau
    , coalesce(markets, 0) as perp_markets
    , coalesce(volume_usd, 0) as perp_volume

    --Cashflow Metrics
    , coalesce(greatest(total_fees, 0), 0) as perp_fees
    , coalesce(greatest(total_fees, 0), 0) as ecosystem_revenue
    
from {{ ref("fact_ostium_metrics") }}
where date < to_date(sysdate())