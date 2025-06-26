{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_SM",
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
        where chain = 'blast'
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    raw_receipt_transactions as (
        SELECT
            parquet_raw:transaction_hash::string as transaction_hash
            , parquet_raw:nonce::string as nonce
            , parquet_raw:block_hash::string as block_hash
            , parquet_raw:block_number::integer as block_number
            , parquet_raw:transaction_index::integer as transaction_index
            , parquet_raw:from_address::string as from_address
            , parquet_raw:to_address::string as to_address
            , parquet_raw:value::integer as value
            , parquet_raw:gas::integer as gas
            , parquet_raw:gas_price::integer as gas_price
            , parquet_raw:input::string as input
            , case
                when try_cast(parquet_raw:max_fee_per_gas::string as integer) is not null
                then try_cast(parquet_raw:max_fee_per_gas::string as integer)
                else 0
            end as max_fee_per_gas
            , case
                when try_cast(parquet_raw:max_priority_fee_per_gas::string as integer) is not null
                then try_cast(parquet_raw:max_priority_fee_per_gas::string as integer)
                else 0
            end as max_priority_fee_per_gas
            , parquet_raw:transaction_type::string as transaction_type
            , parquet_raw:block_timestamp::timestamp_ntz as block_timestamp
            , case
                when try_cast(parquet_raw:receipt_cumulative_gas_used::string as integer) is not null
                then try_cast(parquet_raw:receipt_cumulative_gas_used::string as integer)
                else 0
            end as receipt_cumulative_gas_used
            , case
                when try_cast(parquet_raw:receipt_gas_used::string as integer) is not null
                then try_cast(parquet_raw:receipt_gas_used::string as integer)
                else 0
            end as receipt_gas_used
            , parquet_raw:receipt_contract_address::string as receipt_contract_address
            , parquet_raw:receipt_status::string as receipt_status
            , case
                when try_cast(parquet_raw:receipt_effective_gas_price::string as integer) is not null
                then try_cast(parquet_raw:receipt_effective_gas_price::string as integer)
                else 0
            end as receipt_effective_gas_price
            , case
                when try_cast(parquet_raw:receipt_l1_fee::string as integer) is not null
                then try_cast(parquet_raw:receipt_l1_fee::string as integer)
                else 0
            end as receipt_l1_fee
            , case
                when try_cast(parquet_raw:receipt_l1_fee_scalar::string as integer) is not null
                then try_cast(parquet_raw:receipt_l1_fee_scalar::string as integer)
                else 0
            end as receipt_l1_fee_scalar
            , parquet_raw:receipt_root_hash::string as receipt_root_hash
        from {{ source("PROD_LANDING", "raw_blast_transactions_parquet") }}
        where
            lower(from_address) <> lower('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001')
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    ),
    transactions AS (
        select *
        from raw_receipt_transactions
        qualify row_number() over (partition by transaction_hash order by block_timestamp desc) = 1
    )
select
    transaction_hash as tx_hash,
    coalesce(to_address, t.from_address) as contract_address,
    block_timestamp,
    date_trunc('day', block_timestamp) raw_date,
    t.from_address,
    t.to_address,
    gas,
    (gas * price) gas_usd,
    'blast' as chain,
    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    case
        when t.input = '0x' and t.value > 0
        then 'EOA'
        when new_contracts.category is not null
        then new_contracts.category
        else null
    end as category,
    receipt_effective_gas_price,
    receipt_gas_used,
    receipt_l1_fee,
    receipt_l1_fee_scalar
    -- sybil.user_type,
    -- sybil.address_life_span,
    -- sybil.cur_total_txns,
    -- sybil.cur_distinct_to_address_count,
    -- sybil.probability,
    -- sybil.engagement_type,
    -- bal.balance_usd,
    -- bal.native_token_balance,
    -- bal.stablecoin_balance
from transactions t
left join new_contracts on lower(t.to_address) = lower(new_contracts.address)
left join prices on raw_date = prices.date
-- left join balances as bal on t.from_address = bal.address and raw_date = bal.date