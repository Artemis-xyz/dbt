{% macro clean_flipside_evm_transactions(chain, native_token_coingecko_id='ethereum') %}

with
    prices as ({{ get_coingecko_price_with_latest(native_token_coingecko_id)}})
    , native_transfers as (
        --Genesis transactions
        {% if chain in ('ethereum') %}
            select
                block_timestamp
                , block_number
                , tx_hash as transaction_hash
                , transaction_index
                , 0 as trace_index
                , lower(from_address) as from_address
                , lower(to_address) as to_address
                , value_raw::float as value_raw
                , value_raw / 1e18 as value_native
            from {{ ref("fact_ethereum_genesis_transactions") }}
            where block_timestamp <= '2016-01-01'
            {% if is_incremental() %}
                where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}
            union all
        {% endif %}
        --Native Transfers
        select
            block_timestamp
            , block_number
            , tx_hash as transaction_hash
            , tx_position as transaction_index
            , trace_index
            , from_address
            , to_address
            , value::float as value_raw
            , value / 1e18 as value_native
        from {{ chain }}_flipside.core.fact_traces
        where tx_succeeded
            and trace_succeeded
            and type not in ('DELEGATECALL', 'STATICCALL')
            and block_timestamp <= '2016-01-01'
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        --Gas Fees you pay regardless of success
        select
            block_timestamp
            , block_number
            , tx_hash as transaction_hash
            , tx_position as transaction_index
            , -1 as trace_index
            , from_address
            , to_address
            , gas_used * gas_price::float as value_raw
            , gas_used * gas_price::float / 1e9 as value_native
        from {{ chain }}_flipside.core.fact_transactions
        left join {{ chain }}_flipside.core.fact_blocks using (block_number)
        where block_timestamp <= '2016-01-01'
        {% if is_incremental() %}
            where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)

select 
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , trace_index
    , chain_agnostic_id || ':' || 'native' as contract_address
    , from_address
    , to_address
    , value_raw::float as amount_raw
    , value_native::float as amount_native
    , value_native::float * price as amount
    , price
from native_transfers
left join prices 
    on native_transfers.block_timestamp::date = prices.date
left join {{ref("dim_chain_id_mapping")}} on chain_name = '{{chain}}'
{% endmacro %}

