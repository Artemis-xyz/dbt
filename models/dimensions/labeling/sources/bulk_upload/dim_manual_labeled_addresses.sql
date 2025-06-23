{{config(materialized='table')}}
select
    address
    , name
    , artemis_application_id
    , chain
    , is_token
    , is_fungible
    , type
    , last_updated
from {{ ref("dim_manual_labeled_addresses_changelog") }}
qualify row_number() over (partition by lower(address), chain order by last_updated desc) = 1