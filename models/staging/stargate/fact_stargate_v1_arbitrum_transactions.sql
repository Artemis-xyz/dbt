-- Do I need to set up the snowflake_warehouse? Set db/schema/alias
-- Is alias required for importing in downstream ez tables?

{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE_MD",
        alias="fact_stargate_v1_arbitrum_transactions"
    )
}}

-- TODO add input token
with
    pools as (
        select LOWER(address) AS address
        from (
            values
            ('0x915A55e36A01285A14f05dE6e81ED9cE89772f8e'),
            ('0x892785f33CdeE22A30AEF750F285E18c18040c3e'),
            ('0xB6CfcF89a7B22988bfC96632aC2A9D6daB60d641'),
            ('0xaa4BF442F024820B2C28Cd0FD72b82c63e66F56C'),
            ('0xF39B7Be294cB36dE8c510e267B82bb588705d977'),
            ('0x600E576F9d853c95d58029093A16EE49646F3ca5')
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
from arbitrum_flipside.core.fact_decoded_event_logs
where 1=1
    and contract_address in (select address from pools)
    and event_name in (select name from event_names)
