-- This file has been renamed from uploaded_labeled_addresses to dim_manual_labeled_addresses
-- Hardcoded `all_chains_gas_dau_txns_by_contract_v2` to prevent possible circular dependency
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
where address is not null and chain in (select distinct chain from pc_dbt_db.prod.all_chains_gas_dau_txns_by_contract_v2)
qualify row_number() over (partition by lower(address), chain order by last_updated desc) = 1