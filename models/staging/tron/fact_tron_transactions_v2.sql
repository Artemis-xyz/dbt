{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
    )
}}

with
    tron_fees as (
        select
            concat('0x', transaction_hash) as tx_hash,
            max(amount) as gas,
            max(usd_amount) as gas_usd,
            max(usd_exchange_rate) as price
        from tron_allium.assets.trx_token_transfers
        where
            transfer_type = 'fees'
            {% if is_incremental() %}
                and block_timestamp
                >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
            {% endif %}
        group by transaction_hash
    ),
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
        where chain = 'tron'
    ),
    balances as (
        select address, date, balance_usd, native_token_balance, stablecoin_balance
        from {{ ref("fact_tron_daily_balances") }}
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run 
            where date >= (select dateadd('day', -5, max(raw_date)) from {{ this }})
        {% endif %}
    )
select
    hash as tx_hash,
    coalesce(to_address, t.from_address) as contract_address,
    block_timestamp,
    date_trunc('day', block_timestamp) raw_date,
    t.from_address,
    fees.gas::double as tx_fee,
    fees.gas_usd::double as gas_usd,
    'tron' as chain,
    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    case
        when t.input = '0x' and t.value::double > 0
        then 'EOA'
        when new_contracts.category is not null
        then new_contracts.category
        else null
    end as category,
    bal.balance_usd::double as balance_usd,
    bal.native_token_balance::double as native_token_balance,
    bal.stablecoin_balance::double as stablecoin_balance
from tron_allium.raw.transactions as t
left join tron_fees as fees on t.hash = fees.tx_hash
left join new_contracts on lower(t.to_address) = lower(new_contracts.address)
left join balances as bal on t.from_address = bal.address and raw_date = bal.date
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run 
    where
        block_timestamp
        >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
{% endif %}
