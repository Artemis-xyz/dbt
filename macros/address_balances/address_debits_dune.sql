-- This model currently does not take into account native tokens.
-- TODO: Add support for native tokens
{% macro address_debits_dune(chain) %}
select
    from_address as address,
    contract_address,
    block_timestamp,
    cast(amount_native * -1 as float) as debit,
    null as debit_usd,
    transaction_hash as tx_hash,
    null as trace_index,
    event_index
from {{ ref("fact_" ~ chain ~ "_token_transfers")}}
where
    to_date(block_timestamp) < to_date(sysdate())
    and from_address <> lower('0x0000000000000000000000000000000000000000')
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}