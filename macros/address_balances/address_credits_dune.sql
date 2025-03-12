-- This model currently does not take into account native tokens.
-- TODO: Add support for native tokens
{% macro address_credits_dune(chain) %}
select
    to_address as address,
    contract_address,
    block_timestamp,
    cast(amount_native as float) as credit,
    null as credit_usd,
    tx_hash,
    null as trace_index,
    event_index
from {{ ref("fact_" ~ chain ~ "_token_transfers") }}
where
    to_address <> lower('0x0000000000000000000000000000000000000000')
    and to_date(block_timestamp) < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}