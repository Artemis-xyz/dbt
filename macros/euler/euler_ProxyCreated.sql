{% macro euler_ProxyCreated(chain, contract_address) %}
    select
        block_timestamp
        , transaction_hash
        , event_index
        , decoded_log:"implementation"::string as implementation_address
        , decoded_log:"proxy"::string as proxy_address
        , substr(decoded_log:"trailingData", 0, 42)::string as asset_token_address
        , decoded_log:"trailingData"::string as trailing_data
        , decoded_log:"upgradeable"::number as upgradeable
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }}
    where lower(contract_address) = lower('{{ contract_address }}')
    and event_name = 'ProxyCreated'
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}