{{ config(materialized="table") }}

with data as (
    select 
        source_json:"latest_timestamp"::date as date
        , source_json:"ticker"::string as ticker
        , max_by(source_json:"latest_price"::float, extraction_date) as close_price
    from {{ source("PROD_LANDING" , "raw_realtime_equity_price") }}
    group by source_json:"latest_timestamp"::date, source_json:"ticker" 
)
select 
    date
    , lower(ticker) as ticker
    , close_price
from data
where date >= dateadd(day, -7, to_date(sysdate()))

