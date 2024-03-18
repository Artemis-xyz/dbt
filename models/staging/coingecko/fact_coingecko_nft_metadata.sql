{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_coingecko_nft_metadata") }}
    ),
    nft_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_coingecko_nft_metadata") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date,
            data:id::string as coingecko_nft_id,
            data:contract_address::string as nft_contract_address,
            data:asset_platform_id::string as nft_asset_platform_id,
            data:name::string as nft_name,
            data:symbol::string as nft_symbol,
            data:image:small::string as nft_image_small,
            data:description::string as nft_description,
            data:native_currency::string as nft_native_currency,
            data:native_currency_symbol::string as nft_native_currency_symbol,
            data:floor_price:native_currency::float as nft_floor_price_native_currency,
            data:floor_price:usd::float as nft_floor_price_usd,
            data:market_cap:native_currency::float as nft_market_cap_native_currency,
            data:market_cap:usd::float as nft_market_cap_usd,
            data:volume_24h:native_currency::float as nft_volume_24h_native_currency,
            data:volume_24h:usd::float as nft_volume_24h_usd,
            data:floor_price_in_usd_24h_percentage_change::float
            as nft_floor_price_in_usd_24h_percentage_change,
            data:floor_price_24h_percentage_change:usd::float
            as nft_floor_price_24h_percentage_change_usd,
            data:floor_price_24h_percentage_change:native_currency::float
            as nft_floor_price_24h_percentage_change_native_currency,
            data:market_cap_24h_percentage_change:usd::float
            as nft_market_cap_24h_percentage_change_usd,
            data:market_cap_24h_percentage_change:native_currency::float
            as nft_market_cap_24h_percentage_change_native_currency,
            data:volume_24h_percentage_change:usd::float
            as nft_volume_24h_percentage_change_usd,
            data:volume_24h_percentage_change:native_currency::float
            as nft_volume_24h_percentage_change_native_currency,
            data:number_of_unique_addresses::float as nft_number_of_unique_addresses,
            data:number_of_unique_addresses_24h_percentage_change::float
            as nft_number_of_unique_addresses_24h_percentage_change,
            data:volume_in_usd_24h_percentage_change::float
            as nft_volume_in_usd_24h_percentage_change,
            data:total_supply::float as nft_total_supply,
            data:links:homepage::string as nft_homepage_link,
            data:links:twitter::string as nft_twitter_link,
            data:links:discord::string as nft_discord_link,
            data:floor_price_7d_percentage_change:usd::float
            as nft_floor_price_7d_percentage_change_usd,
            data:floor_price_7d_percentage_change:native_currency::float
            as nft_floor_price_7d_percentage_change_native_currency,
            data:floor_price_14d_percentage_change:usd::float
            as nft_floor_price_14d_percentage_change_usd,
            data:floor_price_14d_percentage_change:native_currency::float
            as nft_floor_price_14d_percentage_change_native_currency,
            data:floor_price_30d_percentage_change:usd::float
            as nft_floor_price_30d_percentage_change_usd,
            data:floor_price_30d_percentage_change:native_currency::float
            as nft_floor_price_30d_percentage_change_native_currency,
            data:floor_price_60d_percentage_change:usd::float
            as nft_floor_price_60d_percentage_change_usd,
            data:floor_price_60d_percentage_change:native_currency::float
            as nft_floor_price_60d_percentage_change_native_currency,
            data:floor_price_1y_percentage_change:usd::float
            as nft_floor_price_1y_percentage_change_usd,
            data:floor_price_1y_percentage_change:native_currency::float
            as nft_floor_price_1y_percentage_change_native_currency
        from nft_data
    )

select
    max(date) as date,
    coingecko_nft_id,
    max_by(nft_contract_address, date) as nft_contract_address,
    max_by(nft_asset_platform_id, date) as nft_asset_platform_id,
    max_by(nft_name, date) as nft_name,
    max_by(nft_symbol, date) as nft_symbol,
    max_by(nft_image_small, date) as nft_image_small,
    max_by(nft_description, date) as nft_description,
    max_by(nft_native_currency, date) as nft_native_currency,
    max_by(nft_native_currency_symbol, date) as nft_native_currency_symbol,
    max_by(nft_floor_price_native_currency, date) as nft_floor_price_native_currency,
    max_by(nft_floor_price_usd, date) as nft_floor_price_usd,
    max_by(nft_market_cap_native_currency, date) as nft_market_cap_native_currency,
    max_by(nft_market_cap_usd, date) as nft_market_cap_usd,
    max_by(nft_volume_24h_native_currency, date) as nft_volume_24h_native_currency,
    max_by(nft_volume_24h_usd, date) as nft_volume_24h_usd,
    max_by(
        nft_floor_price_in_usd_24h_percentage_change, date
    ) as nft_floor_price_in_usd_24h_percentage_change,
    max_by(
        nft_floor_price_24h_percentage_change_usd, date
    ) as nft_floor_price_24h_percentage_change_usd,
    max_by(
        nft_floor_price_24h_percentage_change_native_currency, date
    ) as nft_floor_price_24h_percentage_change_native_currency,
    max_by(
        nft_market_cap_24h_percentage_change_usd, date
    ) as nft_market_cap_24h_percentage_change_usd,
    max_by(
        nft_market_cap_24h_percentage_change_native_currency, date
    ) as nft_market_cap_24h_percentage_change_native_currency,
    max_by(
        nft_volume_24h_percentage_change_usd, date
    ) as nft_volume_24h_percentage_change_usd,
    max_by(
        nft_volume_24h_percentage_change_native_currency, date
    ) as nft_volume_24h_percentage_change_native_currency,
    max_by(nft_number_of_unique_addresses, date) as nft_number_of_unique_addresses,
    max_by(
        nft_number_of_unique_addresses_24h_percentage_change, date
    ) as nft_number_of_unique_addresses_24h_percentage_change,
    max_by(
        nft_volume_in_usd_24h_percentage_change, date
    ) as nft_volume_in_usd_24h_percentage_change,
    max_by(nft_total_supply, date) as nft_total_supply,
    max_by(nft_homepage_link, date) as nft_homepage_link,
    max_by(nft_twitter_link, date) as nft_twitter_link,
    max_by(nft_discord_link, date) as nft_discord_link,
    max_by(
        nft_floor_price_7d_percentage_change_usd, date
    ) as nft_floor_price_7d_percentage_change_usd,
    max_by(
        nft_floor_price_7d_percentage_change_native_currency, date
    ) as nft_floor_price_7d_percentage_change_native_currency,
    max_by(
        nft_floor_price_14d_percentage_change_usd, date
    ) as nft_floor_price_14d_percentage_change_usd,
    max_by(
        nft_floor_price_14d_percentage_change_native_currency, date
    ) as nft_floor_price_14d_percentage_change_native_currency,
    max_by(
        nft_floor_price_30d_percentage_change_usd, date
    ) as nft_floor_price_30d_percentage_change_usd,
    max_by(
        nft_floor_price_30d_percentage_change_native_currency, date
    ) as nft_floor_price_30d_percentage_change_native_currency,
    max_by(
        nft_floor_price_60d_percentage_change_usd, date
    ) as nft_floor_price_60d_percentage_change_usd,
    max_by(
        nft_floor_price_60d_percentage_change_native_currency, date
    ) as nft_floor_price_60d_percentage_change_native_currency,
    max_by(
        nft_floor_price_1y_percentage_change_usd, date
    ) as nft_floor_price_1y_percentage_change_usd,
    max_by(
        nft_floor_price_1y_percentage_change_native_currency, date
    ) as nft_floor_price_1y_percentage_change_native_currency
from flattened_data
where nft_number_of_unique_addresses is not null and nft_total_supply is not null
group by coingecko_nft_id
order by nft_market_cap_usd desc
