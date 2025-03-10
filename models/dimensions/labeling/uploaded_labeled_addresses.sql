-- This file will be deleted when the verification is moved to the API
{{config(materialized='table')}}
select
    --lower all hex addresses
    case when substr(address, 1, 2) = '0x' then lower(address) else address end as address
    , name
    , artemis_application_id
    , chain
    , is_token
    , is_fungible
    , type
    , last_updated
from {{ source("PROD_LANDING", "raw_manually_labeled_addresses_csv") }}
qualify row_number() over (partition by lower(address), chain order by last_updated desc) = 1