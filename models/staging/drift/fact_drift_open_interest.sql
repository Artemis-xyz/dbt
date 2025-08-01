{{ config(materialized="table") }}

with data as (
    select 
        value:"open_interest"::float as open_interest
        , date_trunc('day', to_date(value:"timestamp"::string)) as date
        , extraction_date
    from {{ source("PROD_LANDING", "raw_drift_open_interest") }},
    LATERAL FLATTEN(source_json)
),
latest_data as (
    select 
        max_by(date, extraction_date) as date
        , max_by(open_interest, extraction_date) as open_interest
    from data
    group by date
    order by date desc
),
market_data_open_interest as (
    select 
        min_by(open_interest, end_timestamp) as open_interest
        , min_by(last_price, end_timestamp) as last_price
        , ticker_id as ticker_id
        , date_trunc('day', end_timestamp) as date
    from {{ ref('fact_drift_markets') }}
    where open_interest is not null and product_type = 'PERP'
    group by date, ticker_id
),
latest_open_interest_data as (
    select
        date 
        , sum(open_interest * last_price) as open_interest
    from market_data_open_interest
    group by date
)
select 
    date
    , coalesce(IFF(date <= '2025-07-03', open_interest*2, open_interest),0) as open_interest
from latest_data 
where date <= date(sysdate())
union
select 
    date
    , open_interest
from latest_open_interest_data
where date <= date(sysdate())
