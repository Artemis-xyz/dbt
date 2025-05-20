{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

with a as (
    SELECT
        a.value:date::date as date,
        extraction_date,
        a.value:assetInfo[0]::number as assetInfo_type,
        a.value:assetInfo[1]::string as assetInfo_address,
        a.value:assetInfo[2]::number as assetInfo_decimals,
        a.value:chain::string as chain,
        a.value:decimals::number as decimals,
        a.value:sy_address::string as sy_address,
        a.value:totalSupply::string as total_supply,
        a.value:exchangeRate::string as exchange_rate
    FROM
        landing_database.prod_landing.raw_pendle_sy_info,
        lateral flatten (input => parse_json(source_json)) a
)
SELECT
    date,
    assetinfo_type,
    assetinfo_address,
    assetInfo_decimals,
    chain,
    decimals,
    sy_address,
    total_supply,
    exchange_rate,
    row_number() over (partition by date, assetInfo_type, assetInfo_address, assetInfo_decimals, chain, sy_address order by extraction_date ASC) as num
FROM a
QUALIFY num = 1
