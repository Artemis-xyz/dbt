-- Do I need to set up the snowflake_warehouse? Set db/schema/alias
-- Is alias required for importing in downstream ez tables?

{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE_MD",
        alias="fact_stargate_v1_ethereum_transactions"
    )
}}

-- TODO add input token
with
    pools as (
        select LOWER(address) AS address
        from (
            values
            ('0x101816545F6bd2b1076434B54383a1E633390A2E'),
            ('0xdf0770dF86a8034b3EFEf0A1Bb3c889B8332FF56'),
            ('0x38EA452219524Bb87e18dE1C24D3bB59510BD783'),
            ('0x692953e758c3669290cb1677180c64183cEe374e'),
            ('0x0Faf1d2d3CED330824de3B8200fc8dc6E397850d'),
            ('0xfA0F307783AC21C39E939ACFF795e27b650F6e68'),
            ('0x590d4f8A68583639f215f675F3a259Ed84790580'),
            ('0xE8F55368C82D38bbbbDb5533e7F56AfC2E978CC2'),
            ('0x9cef9a0b1bE0D289ac9f4a98ff317c33EAA84eb8'),
            ('0xd8772edBF88bBa2667ed011542343b0eDDaCDa47'),
            ('0x430Ebff5E3E80A6C58E7e6ADA1d90F5c28AA116d'),
            ('0xa572d137666dcbadfa47c3fc41f15e90134c618c')
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
from ethereum_flipside.core.fact_decoded_event_logs
where 1=1
    and contract_address in (select address from pools)
    and event_name in (select name from event_names)