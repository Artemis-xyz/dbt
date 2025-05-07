{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'dim_migratedtranchepools_addresses'
    )
}}

select distinct contract_address as migratedtranchepool_address from ethereum_flipside.core.ez_decoded_event_logs
where event_name = 'PaymentApplied'
and contract_address <> '0xd52dc1615c843c30f2e4668e101c0938e6007220'