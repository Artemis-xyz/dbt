{{ config(materialized="table") }}

with max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_helium_token_burns") }}
),
latest_data as (
    select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_helium_token_burns") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            flattened.value:"block_date"::string as date,
            flattened.value:"dc_minted"::float as dc_minted,
            flattened.value:"hnt_avg_price"::float as hnt_avg_price,
            flattened.value:"hnt_burned"::float as hnt_burned
        from latest_data, lateral flatten(input => data) as flattened
    )

select 
    date,
    coalesce(dc_minted, 0) as dc_minted,
    coalesce(hnt_avg_price, 0) as hnt_avg_price,
    -1* coalesce(hnt_burned, 0) as hnt_burned
from flattened_data
where date < to_date(sysdate())
order by date desc