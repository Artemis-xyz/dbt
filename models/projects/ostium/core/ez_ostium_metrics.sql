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
    , coalesce(trades, 0) as perp_txns
    , coalesce(traders, 0) as perp_dau
    , coalesce(markets, 0) as perp_markets
    , coalesce(volume_usd, 0) as perp_volume
    , coalesce(greatest(total_fees, 0), 0) as perp_fees
    , coalesce(greatest(total_fees, 0), 0) as gross_protocol_revenue
from {{ ref("fact_ostium_metrics") }}
where date < to_date(sysdate())