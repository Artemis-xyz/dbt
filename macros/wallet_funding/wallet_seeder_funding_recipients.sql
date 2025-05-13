{% macro wallet_seeder_funding_recipients(chain) %}

select
    block_timestamp
    , to_address 
    , tx_hash 
    , trace_index AS index 
    , value AS amount 
    , 'native' as transfer_type
    , '0x0000000000000000000000000000000000000000' as token_address
    , {{ chain }} AS chain
from {{ chain }}_flipside.core.fact_traces
where lower(from_address) IN (
        lower('0xD152f549545093347A162Dce210e7293f1452150') -- disperse app contract
        , lower('0x88888c037DF4527933fa8Ab203a89e1e6E58db70') -- multisend contract 
    )
    and trace_succeeded = true 
    and value > 0
    {% if is_incremental() %}
        and block_timestamp > (select max(block_timestamp) from {{ this }})
    {% endif %}

union all

-- erc20 transfers
select
    block_timestamp
    , to_address
    , tx_hash
    , event_index AS index
    , amount
    , 'erc20' AS transfer_type
    , contract_address AS token_address
    , {{ chain }} AS chain
from {{chain}}_flipside.core.ez_token_transfers
where lower(from_address) IN (
    lower('0xD152f549545093347A162Dce210e7293f1452150') -- disperse app contract
    , lower('0x88888c037DF4527933fa8Ab203a89e1e6E58db70') -- multisend contract 
)
{% if is_incremental() %}
    and block_timestamp > (select max(block_timestamp) from {{ this }})
{% endif %}

{% endmacro %}
