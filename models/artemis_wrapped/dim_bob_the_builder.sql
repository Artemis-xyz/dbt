{{config(materialized='table')}}

select lower(address) as address, 'BOB_THE_BUIDLER' as category, contract_deployments as reason
from {{ ref('agg_base_contract_deployments') }}
where contract_deployments > 5

union all

select lower(address) as address, 'BOB_THE_BUIDLER' as category, contract_deployments as reason
from {{ ref('agg_solana_contract_deployments') }}
where contract_deployments > 5