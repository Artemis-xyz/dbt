{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="SUI_MD",
    )
}}
with 
new_contracts as (
    select distinct
        address,
        contract.name,
        contract.chain,
        contract.category,
        contract.sub_category,
        contract.app,
        contract.friendly_name
    from {{ ref("dim_contracts_gold") }} as contract
    where chain = 'sui'
),
sui_transactions as (
    select 
        transaction_block_digest as transaction_block_digest,
        min_by(package, index) as package,
        array_agg(type) as type_array
    from {{ source('ZETTABLOCKS_SUI', 'transactions') }}
    where block_time < to_date(sysdate())
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and block_time
        >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
    {% endif %}
    group by transaction_block_digest
),
prices as ({{ get_coingecko_price_with_latest("sui") }})
select 
    digest as tx_hash,
    tb.block_time as block_timestamp,
    date_trunc('day', tb.block_time) as raw_date, 
    sender,
    (total_gas_cost + storage_rebate - storage_cost)/10e8 as tx_fee,
    (non_refundable_storage_fee + storage_cost - storage_rebate)/10e8 as native_revenue,
    (total_gas_cost + storage_rebate - storage_cost)/10e8 * price as gas_usd,
    (non_refundable_storage_fee + storage_cost - storage_rebate)/10e8 * price as revenue,
    package,
    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    case 
        when package is null and array_size(type_array) = 2 and ARRAY_CONTAINS('TransferObjects'::variant, type_array)
        then 'EOA'
        else new_contracts.category 
    end as category,
    'sui' as chain,
    status
from {{ source('ZETTABLOCKS_SUI', 'transaction_blocks') }} as tb 
left join sui_transactions as t on lower(digest) = lower(transaction_block_digest)
left join new_contracts on lower(package) = lower(address)
left join prices on raw_date = prices.date
where raw_date < to_date(sysdate())
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and block_timestamp
        >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
    {% endif %}
