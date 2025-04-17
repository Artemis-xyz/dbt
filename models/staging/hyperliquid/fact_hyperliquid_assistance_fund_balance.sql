with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_assistance_fund_holdings") }}
        order by extraction_date desc
        limit 1
    )

    , extracted_assistance_fund as (
        select
            value:timestamp::integer as timestamp
            , value:balance::float as balance
            , value:holders_count::integer as holders_count
            , 'hyperliquid' as app
            , 'hyperliquid' as chain
            , 'DeFi' as category
        from latest_source_json, lateral flatten(input => parse_json(source_json))
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
            , app
            , chain
            , category
        from extracted_assistance_fund
    )
select
    date,
    balance,
    daily_balance,
    holders_count,
    app,
    chain,
    category
from daily_assistance_fund
