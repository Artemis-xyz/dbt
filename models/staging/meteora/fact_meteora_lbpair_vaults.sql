{{
    config(
        materialized="table",
        alias="fact_meteora_lbpair_vaults",
    )
}}

-- if there is a change here, please update the last_updated pinned timestamp

with lb_pair_pools as (
    select address
    from {{ ref('fact_meteora_lbpair_pools') }}
), lb_vaults as (
    select distinct account_address
    from {{ source('SOLANA_FLIPSIDE', 'fact_token_account_owners') }}
    where owner in (select address from lb_pair_pools)
) 
select 
    account_address as address,
    'lb pair pool token vault' as name,
    'meteora' as artemis_application_id,
    'solana' as chain,
    null as is_token,
    null as is_fungible,
    'spot_pool' as type,
    TO_TIMESTAMP_NTZ('2025-06-17 16:00:00') as last_updated
from lb_vaults