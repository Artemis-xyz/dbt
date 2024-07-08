{{ config(materialized="view", snowflake_warehouse="ACALA") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_acala_uniq_daily_signers" ) }}
    ),
    acala_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_acala_uniq_daily_signers" ) }}
        where extraction_date = (select max_date from max_extraction)
    ),
    acala_uniq_daily_signers as (
        select
            to_timestamp(value:date::number / 1000)::date as date,
            value:signer signer,
            'acala' as chain
        from acala_data, lateral flatten(input => data)
    )
select date, signer, chain
from acala_uniq_daily_signers
