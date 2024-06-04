{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_polkadot_uniq_daily_signers" ) }}
    ),
    polkadot_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_polkadot_uniq_daily_signers" ) }}
        where extraction_date = (select max_date from max_extraction)
    ),
    uniqs as (
        select
            to_timestamp(value:date::number / 1000)::date as date,
            value:signer_pub_key signer_pub_key,
            'polkadot' as chain
        from polkadot_data, lateral flatten(input => data)
    )
select date, signer_pub_key, chain
from uniqs