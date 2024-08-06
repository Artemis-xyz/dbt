{{ config(materialized="table", snowflake_warehouse="X_SMALL") }}
with
    realtime_token_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_coingecko_token_realtime_data") }}
        WHERE
            extraction_date > DATEADD('day', -1, SYSDATE()) AND
            extraction_date = (
                SELECT MAX(extraction_date) AS max_date FROM {{ source("PROD_LANDING", "raw_coingecko_token_realtime_data") }}
            )
    ),
    flattened_data as (
        select
            date,
            value:"id"::string as token_id,
            value:"symbol"::string as token_symbol,
            value:"name"::string as token_name,
            value:"image"::string as token_image,
            value:"current_price"::float as token_current_price,
            value:"market_cap"::float as token_market_cap,
            value:"market_cap_rank"::int as token_market_cap_rank,
            value:"fully_diluted_valuation"::float as token_fully_diluted_valuation,
            value:"total_volume"::float as token_total_volume,
            value:"high_24h"::float as token_high_24h,
            value:"low_24h"::float as token_low_24h,
            value:"price_change_24h"::float as token_price_change_24h,
            value:"price_change_percentage_24h"::float
            as token_price_change_percentage_24h,
            value:"market_cap_change_24h"::float as token_market_cap_change_24h,
            value:"market_cap_change_percentage_24h"::float
            as token_market_cap_change_percentage_24h,
            value:"circulating_supply"::float as token_circulating_supply,
            value:"total_supply"::float as token_total_supply,
            value:"max_supply"::float as token_max_supply,
            value:"ath"::float as token_ath,
            value:"ath_change_percentage"::float as token_ath_change_percentage,
            value:"ath_date"::timestamp::date as token_ath_date,
            value:"atl"::float as token_atl,
            value:"atl_change_percentage"::float as token_atl_change_percentage,
            value:"atl_date"::timestamp::date as token_atl_date,
            value:"last_updated"::timestamp::date as token_last_updated
        from realtime_token_data, lateral flatten(input => data)
    ),
    -- Coingecko returns duplicate coingecko_ids for some tokens, so we group by
    -- token_id to remove duplicates
    grouped_data as (
        select
            max(date) as date,
            token_id,
            max(token_symbol) as token_symbol,
            max(token_name) as token_name,
            max(token_image) as token_image,
            avg(token_current_price) as token_current_price,
            avg(token_market_cap) as token_market_cap,
            avg(token_market_cap_rank) as token_market_cap_rank,
            avg(token_fully_diluted_valuation) as token_fully_diluted_valuation,
            avg(token_total_volume) as token_total_volume,
            avg(token_high_24h) as token_high_24h,
            avg(token_low_24h) as token_low_24h,
            avg(token_price_change_24h) as token_price_change_24h,
            avg(token_price_change_percentage_24h) as token_price_change_percentage_24h,
            avg(token_market_cap_change_24h) as token_market_cap_change_24h,
            avg(
                token_market_cap_change_percentage_24h
            ) as token_market_cap_change_percentage_24h,
            avg(token_circulating_supply) as token_circulating_supply,
            avg(token_total_supply) as token_total_supply,
            avg(token_max_supply) as token_max_supply,
            avg(token_ath) as token_ath,
            avg(token_ath_change_percentage) as token_ath_change_percentage,
            max(token_ath_date) as token_ath_date,
            avg(token_atl) as token_atl,
            avg(token_atl_change_percentage) as token_atl_change_percentage,
            max(token_atl_date) as token_atl_date,
            max(token_last_updated) as token_last_updated
        from flattened_data
        group by token_id
    )

select *
from grouped_data
order by token_market_cap_rank asc
