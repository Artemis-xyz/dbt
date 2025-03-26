{{
    config(
        materialized="table",
        snowflake_warehouse="GNOSISPAY",
        database="gnosispay",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with transfer_data as (
    select 
        date_trunc('day', block_timestamp) as date,
        sum(to_decimal(RAW_AMOUNT_PRECISE, 38, 0) / 1e18) AS transfer_volume
    from gnosis_flipside.core.fact_token_transfers
    where lower(contract_address) in (
        lower('0x420CA0f9B9b604cE0fd9C18EF134C705e5Fa3430')
    )
    and lower(to_address) in (
                lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
                lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
    )
    and block_timestamp >= date('2024-09-01')
    group by date
    union all
    select
        date_trunc('day', block_timestamp) as date,
        sum(to_decimal(RAW_AMOUNT_PRECISE, 38, 0) / 1e18) AS transfer_volume
    from gnosis_flipside.core.fact_token_transfers
    where lower(contract_address) in (
        lower('0xcB444e90D8198415266c6a2724b7900fb12FC56E')
    )
    and lower(to_address) in (
                lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
                lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
    )
    and block_timestamp < date('2024-09-01')
    group by date
    union all
    select
        date_trunc('day', block_timestamp) as date,
        sum(to_decimal(RAW_AMOUNT_PRECISE, 38, 0) / 1e18) AS transfer_volume
    from gnosis_flipside.core.fact_token_transfers
    where lower(contract_address) in (
        lower('0x5Cb9073902F2035222B9749F8fB0c9BFe5527108')
    )
    and lower(to_address) in (
                lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
                lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
    )
    group by date
)
select
    date::date as date,
    'gnosis' as chain,
    sum(transfer_volume) as transfer_volume
from transfer_data
group by date, chain
order by date desc