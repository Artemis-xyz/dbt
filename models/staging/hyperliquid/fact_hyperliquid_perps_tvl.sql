{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with base_date as (
    -- First date we ingested data from Hyperliquid API
    select to_date('2025-05-09') as initial_date
)

-- Backfill from DefiLlama for dates before Hyperliquid data begins
, defillama_backfill as (
    select date, tvl
    from {{ ref('fact_defillama_protocol_tvls') }}
    where defillama_protocol_id = 5448
      and date < '2025-04-08'
)

-- Always include fixed snapshot from 2025-05-09 - earliest day we ingested data from Hyperliquid API 
-- (only have 30 days lookback period), so we have data until 2025-04-08
, snapshot_on_may9 as (
    select extraction_date, source_url, source_json
    from {{ source('PROD_LANDING', 'raw_hyperliquid_hlp_tvl') }}
    where date(extraction_date) = '2025-05-09'
)

-- Dynamically include newer data past the 1-month lookback window
, snapshot_after_may9 as (
    select extraction_date, source_url, source_json
    from {{ source('PROD_LANDING', 'raw_hyperliquid_hlp_tvl') }}
    where date(extraction_date) > '2025-05-09'
)

-- Combine both fixed and newer data
, combined_snapshots as (
    select * from snapshot_on_may9
    union all
    select * from snapshot_after_may9
)

, hyperliquid_perps_tvl as (
    select
        to_date(to_timestamp_ntz(value[0]::number / 1000)) as date,
        value[1]::float as tvl
    from combined_snapshots,
         lateral flatten(input => parse_json(source_json):accountValueHistory)
)

-- Combine DefiLlama and Hyperliquid data
, unified_tvl as (
    select date, tvl from hyperliquid_perps_tvl
    union all
    select date, tvl from defillama_backfill
)

, date_spine as (
    select date
    from pc_dbt_db.prod.dim_date_spine
    where date between (select min(date) from unified_tvl) and current_date
)

, joined as (
    select
        spine.date,
        unified_tvl.tvl
    from date_spine spine
    left join unified_tvl on spine.date = unified_tvl.date
)

, forward_filled_tvl as (
    select
        date,
        last_value(tvl ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as tvl
    from joined
)

select 
    date,
    max_by(tvl, date) as tvl,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from forward_filled_tvl
-- date when defilamma starts ingesting hyperliquid HLP
where date >= '2024-12-01'
group by date
order by date desc