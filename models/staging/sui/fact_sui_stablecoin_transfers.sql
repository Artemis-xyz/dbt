{{ 
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="SUI",
    ) 
}}

{% set new_stablecoin_address = var('contract_address', "") %}

select
    block_timestamp
    , date
    , checkpoint as block_number
    , epoch
    , tx_hash
    , from_address
    , to_address
    , lower(to_address) in (
        select distinct (lower(premint_address))
        from {{ ref("fact_sui_stablecoin_premint_addresses") }}
    ) as is_burn
    , lower(from_address) in (
        select distinct (lower(premint_address))
        from {{ ref("fact_sui_stablecoin_premint_addresses") }}
    ) as is_mint
    , coalesce(amount / pow(10, num_decimals), 0) as amount
    , case
        when is_mint then amount / pow(10, num_decimals) when is_burn then -1 * amount / pow(10, num_decimals) else 0
    end as inflow
    , case
        when not is_mint and not is_burn then amount / pow(10, num_decimals) else 0
    end as transfer_volume
    , coin_type as contract_address
    , t2.symbol
    , unique_id
from {{ ref("fact_sui_token_transfers") }} t1
inner join {{ ref("fact_sui_stablecoin_contracts") }} t2 
    on lower(t1.coin_type) = lower(t2.contract_address)
{% if is_incremental() and new_stablecoin_address == '' %} 
    where block_timestamp >= (
        select dateadd('day', -3, max(block_timestamp))
        from {{ this }}
    )
{% endif %}
{% if new_stablecoin_address != '' %}
    where lower(t2.contract_address) = lower('{{ new_stablecoin_address }}')
{% endif %}