{{
    config(
        materialized="table",
        unique_key=["block_timestamp", "address"],
        snowflake_warehouse="TON_MD",
    )
}}

with ton_transfers as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("ez_ton_stablecoin_transfers"),
            ]
        )
    }}
)
select 
    to_address as address,
    contract_address,
    block_timestamp,
    cast(amount as float) as credit,
    tx_hash
from ton_transfers
where 
    block_timestamp < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
