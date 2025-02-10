{% macro stargate_asset_map(chain, token_messaging_address, stg_native_pool_address=None, wrapped_native_token_address=None)%}
with  
{% if chain == "mantle" %}
    stargate_pools as (
        select 
            block_timestamp
            , contract_address as token_messaging_address
            , '0x' || substr(data, 25, 40) as stargate_implementation_pool
        from zksync_dune.{{chain}}.traces
        where topics[0]::string = '0xac53470cf8e9e3d673caa7d47cd7db36c0d38cf37fc147c70c92bc0c1c4734f5' and lower(contract_address) = lower('{{token_messaging_address}}') --tokenMessagingContract
    )
    , deploy_traces as (
        select 
            tx_to_hex as to_address
            , stargate_implementation_pool
            , token_messaging_address
            , input_hex as input
        from zksync_dune.{{chain}}.traces
        inner join stargate_pools on lower(tx_to_hex) = lower(stargate_implementation_pool)
            and type = 'CREATE'
    )
{% else %}
    stargate_pools as (
        select 
            block_timestamp
            , contract_address as token_messaging_address
            , '0x' || substr(data, 27, 40) as stargate_implementation_pool
        from {{chain}}_flipside.core.fact_event_logs
        where topics[0]::string = '0xac53470cf8e9e3d673caa7d47cd7db36c0d38cf37fc147c70c92bc0c1c4734f5' and lower(contract_address) = lower('{{token_messaging_address}}') --tokenMessagingContract
    )
    , deploy_traces as (
        select *
        from {{ chain }}_flipside.core.fact_traces
        inner join stargate_pools on lower(to_address) = lower(stargate_implementation_pool)
            and type = 'CREATE'
    )
{% endif %}

, pool_to_token_map as (
    select 
        stargate_implementation_pool
        , token_messaging_address
        {% if stg_native_pool_address %}
            , case 
                when lower(to_address) = lower('{{stg_native_pool_address}}') then lower('{{wrapped_native_token_address}}')
                else '0x' || SUBSTR(input, -552, 40) 
            end as token_address
        {% else %}
            , '0x' || SUBSTR(input, -552, 40) as token_address
        {% endif %}
    from deploy_traces
)
select
    token_messaging_address
    , stargate_implementation_pool
    , token_address
    , decimals
    , symbol
from pool_to_token_map
left join {{ chain }}_flipside.core.dim_contracts on lower(address) = lower(token_address)
{% endmacro %}