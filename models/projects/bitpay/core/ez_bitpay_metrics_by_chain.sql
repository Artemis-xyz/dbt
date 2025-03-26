{{
    config(
        materialized="table",
        snowflake_warehouse="BITPAY",
        database="bitpay",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    t1 as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
        )
        group by 1
    ),
    t2 as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        )
        group by 1
    ),
    t3 as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x4Fabb145d64652a948d72533023f6E7A623C7C53')
        )
        group by 1
    ),
    t4 as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c')
        )
        group by 1
    ),
    t5 as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x8e870d67f660d95d5be530380d0ec0bd388289e1')
        )
        group by 1
    ),
    t6 as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0x6c3ea9036406852006290770BEdFcAbA0e23A0e8')
        )
        group by 1
    ),
    t7 as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where
        (
            lower(from_address) = lower('0xf2a14015eaa3f9cc987f2c3b62fc93eee41aa5d0')
            and
            lower(contract_address) = lower('0xdAC17F958D2ee523a2206206994597C13D831ec7')
        )
        group by 1
    )

select
    day_start::date as date,
    'ethereum' as chain,
    sum(vol) as transfer_volume
from (
    select * from t1
    union all
    select * from t2
    union all
    select * from t3
    union all
    select * from t4
    union all
    select * from t5
    union all
    select * from t6
    union all
    select * from t7
) final
group by date, chain
order by date desc