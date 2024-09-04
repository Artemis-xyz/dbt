{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="TON",
        database="ton",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    block_timestamp,
    trunc(block_timestamp, 'day') as date,
    null as block_number,
    trace_id as index,
    trace_id,
    tx_hash,
    from_address,
    from_type,
    to_address,
    to_type,
    -- Mint: From: Premint, To: Contract
    from_address in (
        select distinct (premint_address)
            from pc_dbt_db.prod.fact_ton_stablecoin_contracts
        ) 
        and to_address not in (
        select distinct (premint_address)
            from pc_dbt_db.prod.fact_ton_stablecoin_contracts
        )
    as is_mint,
    -- BURN: From: Contract, To: Premint
    from_address not in (
        select distinct (premint_address)
            from pc_dbt_db.prod.fact_ton_stablecoin_contracts
        ) 
        and to_address in (
        select distinct (premint_address)
            from pc_dbt_db.prod.fact_ton_stablecoin_contracts
        )
    as is_burn,
    coalesce(amount / POWER(10, decimal), 0) as amount,
    case
        when is_mint then amount / POWER(10, decimal) when is_burn then -1 * amount / POWER(10, decimal) else 0
    end as inflow,
    case
        when
            not is_mint
            and not is_burn
        then amount / POWER(10, decimal)
        else 0
    end as transfer_volume,
    fact_ton_stablecoin_contracts.symbol,
    fact_ton_stablecoin_contracts.contract_address
from 
    {{ ref('fact_ton_stablecoin_transfers') }} as transfers
where dest_verified and source_verified and account_verified
left join
    pc_dbt_db.prod.fact_ton_stablecoin_contracts
    on lower(transfers.symbol)
    = lower(fact_ton_stablecoin_contracts.symbol)
