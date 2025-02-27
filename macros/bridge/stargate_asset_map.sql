{% macro stargate_asset_map(chain, token_messaging_address, stg_native_pool_address=None, wrapped_native_token_address=None)%}
with  
    stargate_pools as (
        select 
            contract_address as token_messaging_address
            , '0x' || substr(data, 27, 40) as stargate_implementation_pool
        {% if chain == "sei" %}
            from {{chain}}_flipside.core_evm.fact_event_logs
        {% else %}
            from {{chain}}_flipside.core.fact_event_logs
        {% endif %}
        where topics[0]::string = '0xac53470cf8e9e3d673caa7d47cd7db36c0d38cf37fc147c70c92bc0c1c4734f5' and lower(contract_address) = lower('{{token_messaging_address}}') --tokenMessagingContract
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(last_modified_timestamp)) from {{ this }})
        {% endif %}
    )
    , deploy_traces as (
        select *
        {% if chain == "sei" %}
            from {{chain}}_flipside.core_evm.fact_traces
        {% else %}
            from {{ chain }}_flipside.core.fact_traces
        {% endif %}
        inner join stargate_pools on lower(to_address) = lower(stargate_implementation_pool)
            and type = 'CREATE'
    )

    , contract_addr_table as (
        select *
        from {{ ref('dim_coingecko_token_map')}}
    )

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
    , sysdate()::timestamp as last_modified_timestamp
from pool_to_token_map
left join contract_addr_table on lower(contract_address) = lower(token_address)
{% endmacro %}