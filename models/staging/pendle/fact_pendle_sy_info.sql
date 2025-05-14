{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

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
