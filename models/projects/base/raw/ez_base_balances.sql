{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="BASE",
        database="base",
        schema="core",
        alias="ez_balances",
    )
}}

select
    address,
    contract_address,
    block_timestamp,
    balance_token
from {{ref("fact_base_address_balances_by_token")}}
{% if is_incremental() %}
    where block_timestamp >= DATEADD('day', -3, to_date(sysdate()))
{% endif %}
