-- This model currently does not take into account native tokens.
-- We do not have access to traces from quicknode yet.
-- Only use ("celo") address balances for stablecoin data
{% macro address_credits_quicknode(chain) %}
select
    to_address as address,
    contract_address,
    block_timestamp,
    cast(amount as float) as credit,
    null as credit_usd,
    transaction_hash as tx_hash,
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
