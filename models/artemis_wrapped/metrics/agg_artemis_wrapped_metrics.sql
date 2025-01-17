{{config(materialized='view')}}

select 
    address
    , total_txns
    , total_gas_paid
    , days_onchain
    , apps_used
from {{ ref('agg_base_artemis_wrapped_metrics') }}

union all

select 
    address::string as address
    , total_txns
    , total_gas_paid
    , days_onchain
    , apps_used
from {{ ref('agg_solana_artemis_wrapped_metrics') }}