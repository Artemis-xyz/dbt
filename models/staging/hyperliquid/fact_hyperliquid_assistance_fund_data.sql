{{ config(
    materialized="table",
    snowflake_warehouse="HYPERLIQUID",
) }}

with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_assistance_fund_data") }}
        order by extraction_date desc
        limit 1
    ),

    extracted_assistance_fund_data as (
        select
            TO_CHAR(TO_DATE(d.value:"date"::string, 'DD-MM-YYYY'), 'YYYY-MM-DD') as date,
            d.value:"totalAmount"::double as total_amount,
            d.value:"HYPE_total"::double as hype_total,
            d.value:"HYPE_amount"::double as hype_amount
        from latest_source_json, lateral flatten(input => parse_json(source_json)) d
    )
select
    date
    , total_amount as cumulative_hype
    , hype_total as usd_value
    , hype_amount as hype_value
    , 'hyperliquid' as chain
from extracted_assistance_fund_data