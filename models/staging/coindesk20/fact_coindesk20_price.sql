{{ config(snowflake_warehouse="COINDESK_20", materialized="table") }}

with
decoded_coindesk20_data as (
    SELECT 
        TO_DATE(source_json:"TIMESTAMP"::timestamp_ntz) as date,
        source_json:"CLOSE"::FLOAT as price,
        extraction_date
    from {{ source("PROD_LANDING", "raw_historical_coindesk_20") }} t1
)
SELECT 
    date
    , max_by(price, extraction_date) as price
from decoded_coindesk20_data
group by date
