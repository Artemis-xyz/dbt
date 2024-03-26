{{ config(materialized="table") }}
with
    raw_data as (
        select
            extraction_date,
            extraction_date::date - interval '1 DAY' as date,
            parse_json(source_json):"volume"::float / 1E6 as trading_volume
        from {{ source("PROD_LANDING", "raw_drift_trading_volume") }}
    ),
    min_extraction_date as (
        select min(extraction_date) as extraction_date, date from raw_data group by date
    )

select
    raw_data.extraction_date::date - interval '1 DAY' as date,
    'drift' as app,
    'DeFi' as category,
    'solana' as chain,
    trading_volume
from raw_data
left join
    min_extraction_date
    on raw_data.extraction_date = min_extraction_date.extraction_date
    and raw_data.date = min_extraction_date.date
union all
select date, app, category, chain, trading_volume  -- this references historical drift data.
from pc_dbt_db.prod.fact_drift_trading_volume_unique_traders_gold
where market_pair is null
order by date desc
