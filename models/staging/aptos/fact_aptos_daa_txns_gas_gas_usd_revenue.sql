{{ config(materialized="incremental", unique_key='date') }}
with
    apt_prices as ({{ get_coingecko_price_with_latest("aptos") }}),
    single_signed_transactions as (
        select block_timestamp::date as date, parse_json(signature):public_key as signer
        from aptos_flipside.core.fact_transactions
        where
            tx_type = 'user_transaction'
            and success = 'true'
            and parse_json(signature):public_key is not null
            {% if is_incremental() %}
                and block_timestamp::date > (select dateadd('day', -3, max(date)) from {{ this }})
            {% endif %}
    ),
    primary_multi_signed_transactions as (
        select
            block_timestamp::date as date,
            parse_json(signature):sender:public_key as signer
        from aptos_flipside.core.fact_transactions
        where
            tx_type = 'user_transaction'
            and success = 'true'
            and parse_json(signature):sender is not null
            {% if is_incremental() %}
                and block_timestamp::date > (select dateadd('day', -3, max(date)) from {{ this }})
            {% endif %}
    ),
    raw_secondary_multi_signed_transactions as (
        select
            block_timestamp,
            parse_json(signature):secondary_signers as secondary_signers
        from aptos_flipside.core.fact_transactions
        where
            tx_type = 'user_transaction'
            and success = 'true'
            and parse_json(signature):secondary_signers is not null
            {% if is_incremental() %}
                and block_timestamp::date > (select dateadd('day', -3, max(date)) from {{ this }})
            {% endif %}
    ),
    secondary_multi_signed_transactions as (
        select block_timestamp::date as date, value:"public_key" as signer
        from
            raw_secondary_multi_signed_transactions,
            lateral flatten(input => secondary_signers)
        where value:"public_key" is not null
    ),
    raw_bitmap_multi_sig_transactions as (
        select block_timestamp::date as date, value:"public_keys" as public_keys
        from
            raw_secondary_multi_signed_transactions,
            lateral flatten(input => secondary_signers)
    ),
    bitmap_multi_sig_transactions as (
        select date, value as signer
        from raw_bitmap_multi_sig_transactions, lateral flatten(input => public_keys)
    ),
    combined_signers as (
        select date, signer
        from single_signed_transactions
        union all
        select date, signer
        from primary_multi_signed_transactions
        union all
        select date, signer
        from secondary_multi_signed_transactions
        union all
        select date, signer
        from bitmap_multi_sig_transactions
    ),
    dau_data as (
        select date, count(distinct signer) as daa from combined_signers group by date
    ),
    txn_and_gas as (
        select
            block_timestamp::date as date,
            sum(case when success = 'true' then 1 else 0 end) as txns,
            sum(gas_used * gas_unit_price) / 1E8 as gas
        from aptos_flipside.core.fact_transactions
        where tx_type = 'user_transaction'
        {% if is_incremental() %}
            and block_timestamp::date > (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
        group by block_timestamp::date
    ),
    raw as (
        select dau_data.date, daa, txns, gas, 'aptos' as chain
        from dau_data
        left join txn_and_gas on txn_and_gas.date = dau_data.date
    )
select raw.*, gas * price as gas_usd, gas * price as revenue
from raw
left join apt_prices on raw.date = apt_prices.date
where raw.date < to_date(sysdate())
