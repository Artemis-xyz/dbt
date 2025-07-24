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
)
select 
    date
    , coalesce(IFF(date <= '2025-07-03', open_interest*2, open_interest),0) as open_interest
from latest_data
where date <= date(sysdate())
