with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_axie_sales_data") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_axie_sales_data") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            value:"date"::timestamp as date,
            value:"avgPriceUSD"::float as avg_price_usd,
            value:"salesUSD"::float as daily_sales_usd,
            value:"totalTransactions"::int as daily_transactions,
            value:"uniqueBuyers"::int as daily_unique_buyers
        from latest_data, lateral flatten(input => data:salesSummaryRecords) -- fmt: off
    )
select *
from flattened_data
order by date desc
