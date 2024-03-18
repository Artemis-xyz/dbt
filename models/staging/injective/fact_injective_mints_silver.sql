with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_injective_mints") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_injective_mints") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(f.value:date::number / 1000)) as date,
            f.value:staker_rewards::float as mints
        from latest_data, lateral flatten(input => data) as f
    )
select date, mints, 'injective' as chain
from flattened_data
where date < to_date(sysdate())
order by date desc
