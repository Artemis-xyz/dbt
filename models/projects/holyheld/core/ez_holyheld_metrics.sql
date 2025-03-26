{{
    config(
        materialized="table",
        snowflake_warehouse="HOLYHELD",
        database="holyheld",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    polygon as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_polygon_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x3c499c542cef5e3811e1192ce70d8cc03d5c3359')
            and date >= '2023-03-13'
        group by 1
    ),
    avalanche as (
        select
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_avalanche_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and (lower(contract_address) = lower('0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e') 
                or lower(contract_address) = lower('0xC891EB4cbdEFf6e073e859e987815Ed1505c2ACD'))
            and date >= '2023-03-14'
        group by 1
    ),
    ethereum as (
        select 
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_ethereum_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and (lower(contract_address) = lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') 
                or lower(contract_address) = lower('0xdAC17F958D2ee523a2206206994597C13D831ec7') 
                or lower(contract_address) = lower('0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c'))
            and date >= '2023-03-14'
        group by 1
    ),
    optimism as (
        select 
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_optimism_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x0b2c639c533813f4aa9d7837caf62653d097ff85')
            and date >= '2023-03-14'
        group by 1
    ),
    arbitrum as (
        select 
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_arbitrum_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0xaf88d065e77c8cc2239327c5edb3a432268e5831')
            and date >= '2023-03-14'
        group by 1
    ),
    base as (
        select 
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_base_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913')
            and date >= '2023-03-14'
        group by 1
    ),
    bsc as (
        select 
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_bsc_stablecoin_transfers") }}
        where 
            lower(to_address) = lower('0x0146dca5eD7fAc1Dd53A2791089E109645732E1c') 
            and lower(contract_address) = lower('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
            and date >= '2024-08-01'
        group by 1
    )

select 
    day_start::date as date,
    sum(vol) as transfer_volume
from (
    select * from polygon
    union all
    select * from avalanche
    union all
    select * from ethereum
    union all
    select * from optimism
    union all
    select * from arbitrum
    union all
    select * from base
    union all
    select * from bsc
) final_result
group by date
order by date desc