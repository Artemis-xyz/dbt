{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="CELO_LG",
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
        where chain = 'celo'
    ),
    prices as (
        select date, 'eip155:42220:native' as fee_currency, price as price
        from ({{ get_coingecko_price_with_latest("celo") }})
        union all
        select
            date,
            '0x765de816845861e75a25fca122bb6898b8b1282a' as fee_currency,
            price as price
        from ({{ get_coingecko_price_with_latest("celo-dollar") }})
        union all
        select
            date,
            '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73' as fee_currency,
            price as price
        from ({{ get_coingecko_price_with_latest("celo-euro") }})
    )
    , transactions as (
        select
            transaction_hash as tx_hash,
            coalesce(to_address, t.from_address) as contract_address,
            block_timestamp,
            block_timestamp::date as raw_date,
            t.from_address,
            (receipt_gas_used * gas_price) / 1E18 as tx_fee,
            case
                when fee_currency is null then 'eip155:42220:native' else fee_currency
            end as fee_currency,
            'celo' as chain,
            new_contracts.name,
            new_contracts.app,
            new_contracts.friendly_name,
            new_contracts.sub_category,
            case
                when t.input = '0x' and t.value::integer > 0
                then 'EOA'
                when new_contracts.category is not null
                then new_contracts.category
                else null
            end as category,
            null as user_type,
            null as address_life_span,
            null as cur_total_txns,
            null as cur_distinct_to_address_count,
            null as probability,
            null as engagement_type,
            CAST(current_timestamp() AS TIMESTAMP_NTZ) AS last_updated_timestamp
        from {{ ref("fact_celo_transactions") }} as t
        left join new_contracts on lower(t.to_address) = lower(new_contracts.address)
        {% if is_incremental() %}
            where (block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            or new_contracts.last_updated >= (select dateadd('day', -3, max(last_updated_timestamp)) from {{ this }}))
        {% endif %}
    )
select
    tx_hash,
    contract_address,
    block_timestamp,
    raw_date,
    from_address,
    tx_fee,
    tx_fee * price as gas_usd,
    transactions.fee_currency,
    chain,
    name,
    app,
    friendly_name,
    sub_category,
    category,
    user_type,
    address_life_span,
    cur_total_txns,
    cur_distinct_to_address_count,
    probability,
    engagement_type,
    last_updated_timestamp
from transactions
left join prices on raw_date = prices.date and lower(transactions.fee_currency) = lower(prices.fee_currency)