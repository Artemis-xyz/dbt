{% macro token_transfer_events(chain) %}
with 
    prices as ({{get_multiple_coingecko_price_with_latest(chain)}})
    , contract_addresses as (
        select 
            distinct 
            contract_address,
            symbol,
            decimals
        from prices
    ), token_transfers as (
        select
            block_timestamp,
            block_number,
            transaction_hash,
            transaction_index,
            event_index,
            contract_address,
            decoded_log:"from"::string as from_address,
            decoded_log:"to"::string as to_address,
            try_to_number(decoded_log:"value"::string) as amount_raw
        from {{ ref("fact_" ~ chain ~ "_decoded_events") }}
        where topic_zero = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            and data != '0x'
            and amount_raw is not null
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
        {% if chain in ('celo') %}
            union
            select distinct
                t1.block_timestamp,
                t1.block_number,
                t1.transaction_hash,
                t1.transaction_index::number as transaction_index,
                -1 as event_index,
                t1.fee_currency as contract_address,
                t1.from_address,
                t2.miner as to_address,
                t1.receipt_gas_used * t1.gas_price as amount_raw
            from {{ref("fact_celo_transactions")}} t1
            left join {{ref("fact_celo_blocks")}} t2
                using (block_number)
            where fee_currency is not null
            {% if is_incremental() %}
                and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}
        {% endif %}
    )
select 
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , event_index
    , token_transfers.contract_address
    , lower(from_address) as from_address
    , lower(to_address) as to_address
    , amount_raw
    , amount_raw / pow(10, contract_addresses.decimals) as amount_native
    , amount_native * prices.price as amount
    , prices.price
from token_transfers
left join prices
    on token_transfers.block_timestamp::date = prices.date
    and lower(token_transfers.contract_address) = lower(prices.contract_address)
left join contract_addresses 
    on lower(token_transfers.contract_address) = lower(contract_addresses.contract_address)
where amount_raw > 0
{% endmacro %}
