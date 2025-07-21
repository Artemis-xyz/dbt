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
            transaction_hash as tx_hash,
            max(amount) as gas,
            max(usd_amount) as gas_usd,
            max(usd_exchange_rate) as price
        from tron_allium.assets.trx_token_transfers
        where
            transfer_type = 'fees'
            {% if is_incremental() %}
                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
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
            contract.friendly_name,
            contract.last_updated
        from {{ ref("dim_all_addresses_labeled_silver") }} as contract
        where chain = 'tron'
    ),
    transactions_with_contracts as (
        select
            hash as tx_hash,
            coalesce(to_address, t.from_address) as contract_address,
            block_timestamp,
            date_trunc('day', block_timestamp) raw_date,
            t.from_address,
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
            CAST(current_timestamp() AS TIMESTAMP_NTZ) AS last_updated_timestamp,
            fees.gas::double as tx_fee,
            fees.gas_usd::double as gas_usd,
            t.input,
            t.value
        from tron_allium.raw.transactions as t
        left join tron_fees as fees on t.hash = fees.tx_hash
        left join new_contracts on lower(t.to_address) = lower(new_contracts.address)
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run 
            where
                block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    ),

    {% if is_incremental() %}
    new_contracts_incremental as (
        select * from new_contracts 
        where last_updated
                >= (select dateadd('day', -3, max(last_updated_timestamp)) from {{ this }})
    ), 
    updated_contract_transactions as (
        select
            t.tx_hash,
            t.contract_address,
            t.block_timestamp,
            t.raw_date,
            t.from_address,
            t.chain,
            c.name,
            c.app,
            c.friendly_name,
            c.sub_category,
            case
                when t.input = '0x' and t.value::double > 0
                then 'EOA'
                when c.category is not null
                then c.category
                else null
            end as category,
            CAST(current_timestamp() AS TIMESTAMP_NTZ) AS last_updated_timestamp,
            t.tx_fee,
            t.gas_usd,
            t.input,
            t.value
        from {{ this }} as t
        inner join new_contracts_incremental c on lower(t.contract_address) = lower(c.address)
    ),
    {% endif %}

    final_table as (
        select * from transactions_with_contracts
        {% if is_incremental() %}
            union all
            select u.* 
            from updated_contract_transactions u
            where not exists (
                select 1 
                from transactions_with_contracts t 
                where t.tx_hash = u.tx_hash
            )
        {% endif %}
    )

select 
    *,
    null as balance_usd, -- got from balances table for sybil model
    null as native_token_balance, -- got from balances table for sybil model
    null as stablecoin_balance, -- got from balances table for sybil model
    null as user_type,
    null as probability,
    null as engagement_type
from final_table
