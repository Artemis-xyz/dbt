{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_zksync_data") }}
    ),
    zksync_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_zksync_data") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    date(left(value:"date_time"::string, 10)) as date,
    value:unique_active_users as daa,
    value:all_transactions as txns,
    coalesce(value:txn_fees, 0) as gas,
    coalesce(value:txn_fees_usd, 0) as gas_usd,
    value as source,
    'zksync' as chain
from zksync_data, lateral flatten(input => data:data:records)
