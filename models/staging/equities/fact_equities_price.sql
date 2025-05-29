{{ config(materialized="table") }}
select 
    source_json:"unique_id"::string as unique_id
    , lower(source_json:"ticker"::string) as ticker
    , source_json:"date"::date as date
    , source_json:"adj_close"::float as adj_close
    , source_json:"adj_factor"::float as adj_factor
    , source_json:"adj_high"::float as adj_high
    , source_json:"adj_low"::float as adj_low
    , source_json:"adj_open"::float as adj_open
    , source_json:"adj_volume"::float as adj_volume
    , source_json:"close"::float as close
    , source_json:"ex_dividend"::float as ex_dividend
    , source_json:"high"::float as high
    , source_json:"low"::float as low
    , source_json:"open"::float as open
    , source_json:"split_ratio"::float as split_ratio
    , source_json:"volume"::float as volume
from {{ source("PROD_LANDING" , "intrinio_historical_equity_prices") }}
qualify row_number() over (partition by source_json:"date", lower(source_json:"ticker") order by extraction_date desc) = 1

