{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
    )
}}

with
    new_contracts as (
        select distinct
            address,
            contract.name,
            contract.chain,
            contract.artemis_category_id as category,
            contract.artemis_sub_category_id as sub_category,
            contract.artemis_application_id as app,
            contract.friendly_name
        from {{ ref("dim_all_addresses_labeled_gold") }} as contract
        where chain = 'mantle'
    ),
    prices as ({{ get_coingecko_price_with_latest("mantle") }})
select
    hash_hex as tx_hash,
    coalesce(to_hex, t.from_hex) as contract_address,
    CONVERT_TIMEZONE('UTC', block_time) as block_timestamp,
    block_timestamp::date raw_date,
    t.from_hex as from_address,
    l1_fee / 1E18 + gas_price * gas_used/1E18 as tx_fee,
    (tx_fee * price) gas_usd,
    'mantle' as chain,
    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    case
        when t.data_hex = '0x' and t.value > 0
        then 'EOA'
        when new_contracts.category is not null
        then new_contracts.category
        else null
    end as category
    
from zksync_dune.mantle.transactions as t
left join new_contracts on lower(t.to_hex) = lower(new_contracts.address)
left join prices on raw_date = prices.date
where
    lower(t.from_hex) <> lower('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001')
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and block_time
        >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
    {% endif %}
