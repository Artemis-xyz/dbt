-- Do I need to set up the snowflake_warehouse? Set db/schema/alias
-- Is alias required for importing in downstream ez tables?

{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE_MD",
        alias="fact_stargate_v1_avalanche_transactions"
    )
}}

-- TODO add input token
with
    pools as (
        select LOWER(address) AS address
        from (
            values
            ('0x1205f31718499dBf1fCa446663B532Ef87481fe1'),
            ('0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c'),
            ('0x1c272232Df0bb6225dA87f4dEcD9d37c32f63Eea'),
            ('0x8736f92646B2542B3e5F3c63590cA7Fe313e283B'),
            ('0xEAe5c2F6B25933deB62f754f239111413A0A25ef')
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
from avalanche_flipside.core.fact_decoded_event_logs
where 1=1
    and contract_address in (select address from pools)
    and event_name in (select name from event_names)
