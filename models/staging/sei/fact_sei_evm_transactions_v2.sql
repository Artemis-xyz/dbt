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
        where chain = 'sei'
    ),
    prices as ({{ get_coingecko_price_with_latest("sei-network") }}),
    sei_transactions as ( 
        select
            tx_hash,
            status,
            block_timestamp,
            date_trunc('day', block_timestamp) raw_date,
            t.from_address,
            tx_fee,
            (tx_fee * price) gas_usd,
            new_contracts.address as contract_address,
            new_contracts.name,
            new_contracts.app,
            new_contracts.friendly_name,
            new_contracts.sub_category,
            inserted_timestamp,
            case
                when t.input_data = '0x' and t.value > 0
                then 'EOA'
                when new_contracts.category is not null
                then new_contracts.category
                else null
            end as category
        from sei_flipside.core_evm.fact_transactions as t
        left join new_contracts on lower(t.to_address) = lower(new_contracts.address)
        left join prices on raw_date = prices.date
        {% if is_incremental() %}
        where
            -- this filter will only be applied on an incremental run 
            inserted_timestamp
            >= (select dateadd('day', -5, max(inserted_timestamp)) from {{ this }})
        {% endif %}
    )
    select 
        tx_hash,
        max(status) as status,
        max(block_timestamp) as block_timestamp,
        max(raw_date) as raw_date,
        max(from_address) as from_address,
        max(tx_fee) as tx_fee,
        max(gas_usd) as gas_usd,
        'sei' as chain,
        max(contract_address) as contract_address,
        max(name) as name,
        max(app) as app,
        max(friendly_name) as friendly_name,
        max(sub_category) as sub_category,
        max(inserted_timestamp) as inserted_timestamp,
        max(category) as category,
        null as user_type,
        null as address_life_span,
        null as cur_total_txns,
        null as cur_distinct_to_address_count,
        null as probability,
        null as engagement_type,
        null as balance_usd,
        null as native_token_balance,
        null as stablecoin_balance
    from sei_transactions
    group by tx_hash
