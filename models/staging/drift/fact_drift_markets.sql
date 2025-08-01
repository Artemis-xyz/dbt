{{ config(materialized="table") }}

SELECT
    f.value:"base_currency"::string as base_currency,
    CASE WHEN f.value:"base_volume"::string = 'N/A' THEN NULL ELSE f.value:"base_volume"::float END as base_volume,
    CASE WHEN f.value:"contract_index"::string = 'N/A' THEN NULL ELSE f.value:"contract_index"::number END as contract_index,
    to_timestamp(f.value:"end_timestamp"::number / 1000) as end_timestamp,
    CASE WHEN f.value:"funding_rate"::string = 'N/A' THEN NULL ELSE f.value:"funding_rate"::float END as funding_rate,
    CASE WHEN f.value:"high"::string = 'N/A' THEN NULL ELSE f.value:"high"::float END as high,
    f.value:"index_currency"::string as index_currency,
    f.value:"index_name"::string as index_name,
    CASE WHEN f.value:"index_price"::string = 'N/A' THEN NULL ELSE f.value:"index_price"::float END as index_price,
    CASE WHEN f.value:"last_price"::string = 'N/A' THEN NULL ELSE f.value:"last_price"::float END as last_price,
    CASE WHEN f.value:"low"::string = 'N/A' THEN NULL ELSE f.value:"low"::float END as low,
    CASE WHEN f.value:"next_funding_rate"::string = 'N/A' THEN NULL ELSE f.value:"next_funding_rate"::float END as next_funding_rate,
    CASE WHEN f.value:"next_funding_rate_timestamp"::string = 'N/A' THEN NULL ELSE to_timestamp(f.value:"next_funding_rate_timestamp"::number / 1000) END as next_funding_rate_timestamp,
    CASE WHEN f.value:"open_interest"::string = 'N/A' THEN NULL ELSE f.value:"open_interest"::float END as open_interest,
    f.value:"product_type"::string as product_type,
    f.value:"quote_currency"::string as quote_currency,
    CASE WHEN f.value:"quote_volume"::string = 'N/A' THEN NULL ELSE f.value:"quote_volume"::number END as quote_volume,
    CASE WHEN f.value:"start_timestamp"::string = 'N/A' THEN NULL ELSE to_timestamp(f.value:"start_timestamp"::number / 1000) END as start_timestamp,
    f.value:"ticker_id"::string as ticker_id
FROM landing_database.prod_landing.raw_drift_market_data as dm,
LATERAL FLATTEN(input => dm.source_json:"contracts") as f
