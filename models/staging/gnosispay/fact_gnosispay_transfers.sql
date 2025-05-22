{{
    config(
        materialized="table",
        snowflake_warehouse="GNOSISPAY"
    )
}}

select 
    date_trunc('day', block_timestamp) as date,
    from_address,
    'EURe' as token,
    to_decimal(RAW_AMOUNT_PRECISE, 38, 0) / 1e18 AS transfer_volume
from gnosis_flipside.core.ez_token_transfers
where lower(contract_address) in (
    lower('0x420CA0f9B9b604cE0fd9C18EF134C705e5Fa3430')
)
and lower(to_address) in (
            lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
            lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
)
and block_timestamp >= date('2024-09-01')
union all
select
    date_trunc('day', block_timestamp) as date,
    from_address,
    'EURe' as token,
    to_decimal(RAW_AMOUNT_PRECISE, 38, 0) / 1e18 AS transfer_volume
from gnosis_flipside.core.ez_token_transfers
where lower(contract_address) in (
    lower('0xcB444e90D8198415266c6a2724b7900fb12FC56E')
)
and lower(to_address) in (
            lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
            lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
)
and block_timestamp < date('2024-09-01')
union all
select
    date_trunc('day', block_timestamp) as date,
    from_address,
    'GBPe' as token,
    to_decimal(RAW_AMOUNT_PRECISE, 38, 0) / 1e18 AS transfer_volume
from gnosis_flipside.core.ez_token_transfers
where lower(contract_address) in (
    lower('0x5Cb9073902F2035222B9749F8fB0c9BFe5527108')
)
and lower(to_address) in (
            lower('0x4822521E6135CD2599199c83Ea35179229A172EE'), -- Gnosis Pay aggregator
            lower('0x3d4fd6a1a7a1382ae1d62c3dd7247254a0236847')  -- Gnosis Pay sales address
)