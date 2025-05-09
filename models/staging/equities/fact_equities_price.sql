{{ config(materialized="table") }}

with data as (
    select 
        source_json:"date"::date as date
        , source_json:"ticker"::string as ticker
        , max_by(source_json:"value"::float, extraction_date) as close_price
    from {{ source("PROD_LANDING" , "raw_historical_equity_price") }}
    group by source_json:"date", source_json:"ticker" 
)
select 
    date
    , lower(ticker) as ticker
    , close_price
    , lower(ticker) || '|' || date as unique_id
from data
