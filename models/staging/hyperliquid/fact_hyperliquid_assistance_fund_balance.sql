{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with
    date_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_assistance_fund_holdings") }}
        where date(extraction_date) = '2025-07-28'
    )
    , latest_entry_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_assistance_fund_holdings") }}
        order by extraction_date desc
        limit 1
    )
    , combined_json as (
        select * from date_source_json
        union
        select * from latest_entry_json
    )
    , extracted_assistance_fund as (
        select
            distinct
            flattened.value:timestamp::integer as timestamp,
            flattened.value:balance::float as balance,
            flattened.value:holders_count::integer as holders_count
        from combined_json
        , lateral flatten(input => parse_json(source_json)) as flattened
    )
    , daily_assistance_fund as (
        select
            date(timestamp) as date
            , balance
            , case 
                when lag(balance) over (order by date) is null then balance
                else round(balance - lag(balance) over (order by date), 2)
            end as daily_balance
            , holders_count
        from extracted_assistance_fund
    )
select
    date,
    balance,
    daily_balance,
    holders_count,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from daily_assistance_fund
