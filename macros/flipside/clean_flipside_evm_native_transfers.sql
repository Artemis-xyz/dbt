{% macro clean_flipside_evm_native_token_transfers(chain, native_token_coingecko_id='ethereum') %}

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
            {% if is_incremental() %}
                where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}
            union all
        -- Block Rewards
        -- TODO: Add block rewards
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
            , value_precise_raw::float as value_raw
            , value_raw / 1e18 as value_native
        from {{ chain }}_flipside.core.fact_traces
        where tx_succeeded
            and trace_succeeded
            and type not in ('DELEGATECALL', 'STATICCALL')
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
    , case 
        -- No pricing for Ethereum Genesis transactions
        when '{{chain}}' = 'ethereum' and block_timestamp::date <= '2015-07-29' then value_native::float * coalesce(price, .31) 
        else value_raw::float * price
    end as amount
    , case 
        -- No pricing for Ethereum Genesis transactions
        when '{{chain}}' = 'ethereum' and block_timestamp::date <= '2015-07-29' then coalesce(price, .31) 
        else price
    end as price
from native_transfers
left join prices 
    on native_transfers.block_timestamp::date = prices.date
left join {{ref("dim_chain_id_mapping")}} on chain_name = '{{chain}}'
where block_timestamp::date < to_date(sysdate()) and amount_raw > 0
{% endmacro %}

