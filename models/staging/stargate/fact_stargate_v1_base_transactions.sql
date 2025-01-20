-- Do I need to set up the snowflake_warehouse? Set db/schema/alias
-- Is alias required for importing in downstream ez tables?

{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE_MD",
        alias="fact_stargate_v1_base_transactions"
    )
}}

-- TODO add input token
with
    pools as (
        select LOWER(address) AS address
        from (
            values
            ('0x28fc411f9e1c480AD312b3d9C60c22b965015c6B'),
            ('0x4c80E24119CFB836cdF0a6b53dc23F04F7e652CA')
        ) AS addresses(address)
    ),

    event_signatures as (
        select *
        from (
            values
            ('Mint(address,uint256,uint256,uint256)'),
            ('Burn(address,uint256,uint256)'),
            ('Swap(uint16,uint256,address,uint256,uint256,uint256,uint256,uint256)'),
            ('SwapRemote(address,uint256,uint256,uint256)')
        ) AS signatures(string)
    ),

    event_names as (
        select LEFT(string, CHARINDEX('(', string) - 1) AS name
        from event_signatures
    )

select
    contract_address,
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    decoded_log
from base_flipside.core.fact_decoded_event_logs
where 1=1
    and contract_address in (select address from pools)
    and event_name in (select name from event_names)
