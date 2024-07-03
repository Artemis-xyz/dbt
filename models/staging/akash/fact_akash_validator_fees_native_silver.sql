
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_akash_validator_fees") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_akash_validator_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(value:date::number / 1000)) as date,
            value:"daily_gas_fees_akt"::float as validator_fees_native
        from latest_data, lateral flatten(input => data)
    )
select date, coalesce(validator_fees_native, 0) as validator_fees_native
from flattened_data
where date < to_date(sysdate())
order by date desc
