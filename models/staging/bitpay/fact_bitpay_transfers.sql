{{
    config(
        materialized="table",
        snowflake_warehouse="BITPAY"
    )
}}

with
    usdc as (
        select
            date_trunc('day', date) as date,
            'USDC' as token,
            transfer_volume
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') -- USDC
        )
    ),
    dai as (
        select
            date_trunc('day', date) as date,
            'DAI' as token,
            transfer_volume
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') -- DAI
        )
    ),
    busd as (
        select
            date_trunc('day', date) as date,
            'BUSD' as token,
            transfer_volume
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x4Fabb145d64652a948d72533023f6E7A623C7C53') -- BUSD
        )
    ),
    eurc as (
        select
            date_trunc('day', date) as date,
            'EURC' as token,
            transfer_volume
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c') -- EURC
        )
    ),
    usdp as (
        select
            date_trunc('day', date) as date,
            'USDP' as token,
            transfer_volume
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x8e870d67f660d95d5be530380d0ec0bd388289e1') -- USDP
        )
    ),
    pyusd as (
        select
            date_trunc('day', date) as date,
            'PYUSD' as token,
            transfer_volume
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x6c3ea9036406852006290770BEdFcAbA0e23A0e8') -- PYUSD
        )
    ),
    usdt as (
        select
            date_trunc('day', date) as date,
            'USDT' as token,
            transfer_volume
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0xdAC17F958D2ee523a2206206994597C13D831ec7') -- USDT
        )
    )

select * from usdc
union all
select * from dai
union all
select * from busd
union all
select * from eurc
union all
select * from usdp
union all
select * from pyusd
union all
select * from usdt
