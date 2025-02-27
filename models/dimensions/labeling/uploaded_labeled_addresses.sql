-- This file will be deleted when the verification is moved to the API
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
from {{ source("PROD_LANDING", "raw_manually_labeled_addresses_csv") }}