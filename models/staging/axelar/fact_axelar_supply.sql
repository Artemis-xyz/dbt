{{
    config(
        materialized="table",
        snowflake_warehouse="AXELAR"
    )
}}
    
with max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_axelar_supply_data") }}
), 

supply_data as (
    select
        value:timestamp::date as date,
        value:circulatingSupply::float as circulatingSupply, 
        value:maxSupply::float as maxSupply, 
        value:totalBurned::float as totalBurned,
    from {{ source("PROD_LANDING", "raw_axelar_supply_data") }}, 
        lateral flatten(input => parse_json(source_json))
    where extraction_date = (select max_date from max_extraction)
), 

raw_data as (
    select 
        date,
        circulatingsupply,
        maxsupply,
        totalburned,
        lag(circulatingsupply) over (order by date) as prev_circulating
    from supply_data
),

with_filtered_circulating as (
    select
        date,
        case 
            when prev_circulating is null then circulatingsupply
            when circulatingsupply >= prev_circulating * 0.9 then circulatingsupply
            else null
        end as filtered_circulatingsupply,
        case when maxsupply != 0 then maxsupply else null end as maxsupply,
        case when totalburned != 0 then totalburned else null end as totalburned
    from raw_data
),

with_filled as (
    select
        date,
        last_value(filtered_circulatingsupply ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as circulatingsupply,
        last_value(maxsupply ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as maxsupply,
        last_value(totalburned ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as totalburned
    from with_filtered_circulating
)

select * 
from with_filled
order by date desc