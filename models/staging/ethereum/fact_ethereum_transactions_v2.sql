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
            contract.friendly_name,
            contract.last_updated
        from {{ ref("dim_all_addresses_labeled_silver") }} as contract
        where chain = 'ethereum'
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    balances as (
        select address, date, balance_usd, native_token_balance, stablecoin_balance
        from {{ ref("fact_ethereum_daily_balances") }}
        where
            date < to_date(sysdate())
            {% if is_incremental() %}
                -- this filter will only be applied on an incremental run 
                and date >= (select dateadd('day', -3, max(raw_date)) from {{ this }})
            {% endif %}
    )
select
    tx_hash,
    coalesce(to_address, t.from_address) as contract_address,
    block_timestamp,
    date_trunc('day', block_timestamp) raw_date,
    t.from_address,
    tx_fee,
    (tx_fee * price) gas_usd,
    'ethereum' as chain,
    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    case
        when t.input_data = '0x' and t.value > 0
        then 'EOA'
        when new_contracts.category is not null
        then new_contracts.category
        else null
    end as category,
    sybil.user_type,
    sybil.address_life_span,
    sybil.cur_total_txns,
    sybil.cur_distinct_to_address_count,
    sybil.probability,
    sybil.engagement_type,
    bal.balance_usd,
    bal.native_token_balance,
    bal.stablecoin_balance,
    CAST(current_timestamp() AS TIMESTAMP_NTZ) AS last_updated_timestamp
from ethereum_flipside.core.fact_transactions as t
left join new_contracts on lower(t.to_address) = lower(new_contracts.address)
left join prices on raw_date = prices.date
left join
    {{ ref("dim_ethereum_sybil_address") }} as sybil
    on t.from_address = sybil.from_address
left join balances as bal on t.from_address = bal.address and raw_date = bal.date
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run 
    where
        block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        or 
        new_contracts.last_updated
            >= (select dateadd('day', -3, max(last_updated_timestamp)) from {{ this }})
{% endif %}
