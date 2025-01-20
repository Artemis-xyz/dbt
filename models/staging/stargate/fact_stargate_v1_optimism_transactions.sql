-- Do I need to set up the snowflake_warehouse? Set db/schema/alias
-- Is alias required for importing in downstream ez tables?

{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE_MD",
        alias="fact_stargate_v1_optimism_transactions"
    )
}}

-- TODO add input token
with
    pools as (
        select LOWER(address) AS address
        from (
            values
            ('0xd22363e3762cA7339569F3d33EADe20127D5F98C'),
            ('0xDecC0c09c3B5f6e92EF4184125D5648a66E35298'),
            ('0x165137624F1f692e69659f944BF69DE02874ee27'),
            ('0x368605D9C6243A80903b9e326f1Cddde088B8924'),
            ('0x2F8bC9081c7FCFeC25b9f41a50d97EaA592058ae'),
            ('0x3533F5e279bDBf550272a199a223dA798D9eff78'),
            ('0x5421FA1A48f9FF81e4580557E86C7C0D24C1803')
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
from optimism_flipside.core.fact_decoded_event_logs
where 1=1
    and contract_address in (select address from pools)
    and event_name in (select name from event_names)
