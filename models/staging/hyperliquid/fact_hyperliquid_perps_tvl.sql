{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with 
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_hlp_tvl") }}
        order by extraction_date desc
        limit 1
    ) 

    , hyperliquid_perps_tvl as (
        select
            to_date(to_timestamp_ntz(value[0]::number / 1000)) as date,
            value[1]::float as account_value
        from latest_source_json,
             lateral flatten(input => parse_json(source_json):accountValueHistory)
    )

    , date_spine as (
        select
            date
        from {{ ref("dim_date_spine") }}
        where date between (select min(date) from hyperliquid_perps_tvl) and to_date(sysdate())
    )

    , joined as (
        select
            spine.date
            , tvl.account_value as tvl
        from date_spine spine
        left join hyperliquid_perps_tvl tvl
        on spine.date = tvl.date
    )

    , forward_filled_hyperliquid_perps_tvl as (
        select
            date
            , last_value(tvl ignore nulls) over (
                order by date
                rows between unbounded preceding and current row
            ) as tvl
        from joined
    )

select 
    date
    , max_by(tvl, date) as tvl
    , 'hyperliquid' as app
    , 'hyperliquid' as chain
    , 'DeFi' as category
from forward_filled_hyperliquid_perps_tvl
group by date
order by date desc