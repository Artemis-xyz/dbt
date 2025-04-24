{% macro clean_goldsky_evm_native_token_transfers(chain, native_token_coingecko_id='ethereum') %}

with
    prices as ({{ get_coingecko_price_with_latest(native_token_coingecko_id)}})
    , native_transfers as (
        --Native Transfers
        select
            block_timestamp
            , block_number
            , transaction_hash
            , transaction_index
            , trace_address::string as trace_index
            , from_address
            , to_address
            , value::float as value_raw
            , value::float / 1e18 as value_native
        from {{ref("fact_"~chain~"_traces")}}
        where status = 1
            and call_type not in ('delegatecall', 'staticcall')
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        --Gas Fees you pay regardless of success
        select
            block_timestamp
            , block_number
            , transaction_hash
            , transaction_index
            , '-1' as trace_index
            , from_address
            , miner as to_address
            , gas_used::float * gas_price::float as value_raw
            , gas_used::float * gas_price::float / 1e18 as value_native
        from {{ref("fact_"~chain~"_transactions")}}
        left join {{ref("fact_"~chain~"_blocks")}} using (block_number)
        {% if chain in ('celo') %}
            where fee_currency is null
        {% endif %}
        {% if is_incremental() %}
            {% if chain in ('celo') %} and {% else %} where {% endif %}
            block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)

select 
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , trace_index::string as trace_index
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
where block_timestamp::date < to_date(sysdate())
    and value_raw > 0
{% endmacro %}

