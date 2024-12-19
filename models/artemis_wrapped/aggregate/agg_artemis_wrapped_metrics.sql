{{config(materialized='view')}}

select 
    address
    , total_txns
    , total_txns_percent_rank
    , total_gas_paid
    , total_gas_paid_percent_rank
    , days_onchain
    , days_onchain_percent_rank
    , apps_used
    , apps_used_percent_rank
from {{ ref('agg_base_artemis_wrapped_metrics') }}