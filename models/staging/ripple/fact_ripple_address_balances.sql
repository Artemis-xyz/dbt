{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="RIPPLE",
    )
}}

with ranked_balances as (
    select
        datetime as block_timestamp,
        ledger_index as block_number,
        token_address as contract_address,
        account_address as address,
        balance as balance_raw,
        row_number() over (
            partition by account_address, token_address, datetime
            order by ledger_index desc
        ) as rnk
    from {{ source("SONARX_XRP_BALANCES", "token_balances") }}
    {% if is_incremental() %}
        where datetime >= (select dateadd(day, -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)

select block_timestamp, block_number, contract_address, address, balance_raw
from ranked_balances
where rnk = 1
