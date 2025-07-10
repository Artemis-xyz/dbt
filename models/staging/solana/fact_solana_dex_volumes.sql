-- This query is used to calculate the daily volume of dex swaps on Solana.
-- It excludes marginfi flash loans and is used to calculate the daily volume of dex swaps on Solana.

{{ config(materialized="table", snowflake_warehouse="SOLANA") }}

with scaled_down_volume as (
    select
        date_trunc('day', block_timestamp) as date, 
        sum(
            case
                when swap_from_amount_usd is not null and swap_to_amount_usd is not null then swap_to_amount_usd
                else coalesce(swap_from_amount_usd, swap_to_amount_usd)
            end
        ) as trading_volume
    from solana_flipside.defi.ez_dex_swaps
    where
        greatest(
            coalesce(swap_from_amount_usd, 0),
            coalesce(swap_to_amount_usd, 0)
        )
        /
        nullif(
            least(
                coalesce(swap_from_amount_usd, 0),
                coalesce(swap_to_amount_usd, 0)
            ),
            0
        ) < 50 
        and swap_program in (
            'raydium constant product market maker',
            'raydium concentrated liquidity',
            'Raydium Liquidity Pool V4',
            'raydium liquidity pool program id v5',
            'meteora dlmm pools program', 
            'meteora pools program',
            'phoenix'
        )
    group by date
    order by date asc
), 

pump_fun_volume as (
    select
        date_trunc('day', block_timestamp) as date, 
        sum(
            case
                when swap_from_amount_usd is not null and swap_to_amount_usd is not null then swap_to_amount_usd
                else coalesce(swap_from_amount_usd, swap_to_amount_usd)
            end
        ) as trading_volume
    from solana_flipside.defi.ez_dex_swaps
    where swap_program = 'pump.fun'
    group by date
    order by date asc
), 

excluded_marginfi_volume as (
    with all_marginfi_flash_loans as (
        select *
        from solana_flipside.core.ez_events_decoded
        where program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' and event_type = 'lendingAccountStartFlashloan'
    )
    
    select
        date_trunc('day', block_timestamp) as date, 
        sum(
            case
                when swap_from_amount_usd is not null and swap_to_amount_usd is not null then swap_to_amount_usd
                else coalesce(swap_from_amount_usd, swap_to_amount_usd)
            end
        ) as trading_volume
        
    from solana_flipside.defi.ez_dex_swaps
    where tx_id not in (select tx_id from all_marginfi_flash_loans) 
        and greatest(
                coalesce(swap_from_amount_usd, 0),
                coalesce(swap_to_amount_usd, 0)
            )
            /
            nullif(
                least(
                    coalesce(swap_from_amount_usd, 0),
                    coalesce(swap_to_amount_usd, 0)
                ),
                0
            ) < 50 
        and swap_program not in (
            'raydium constant product market maker',
            'raydium concentrated liquidity',
            'Raydium Liquidity Pool V4',
            'raydium liquidity pool program id v5', 
            'meteora dlmm pools program', 
            'meteora pools program',
            'phoenix', 
            'pump.fun', 
            'orca token swap', 
            'ORCA Token Swap V2', 
            'orca whirlpool program'
        )
    group by date
    order by date asc 
), 

orca_volume as (
    select 
        date,
        trading_volume
    from orca.prod_core.ez_metrics
    order by date asc
), 

lifinity_volume as (
    select
        date, 
        daily_volume as trading_volume
    from pc_dbt_db.prod.fact_lifinity_dex_volumes   
    order by date asc
)

select
    coalesce(sd.date, pump.date, margin.date, orca.date, lifinity.date) as date, 
    sum((coalesce(sd.trading_volume,0) + 
        coalesce(pump.trading_volume,0) + 
        coalesce(margin.trading_volume,0) + 
        coalesce(orca.trading_volume,0) + 
        coalesce(lifinity.trading_volume,0))) as daily_volume_usd
from scaled_down_volume as sd
full join pump_fun_volume as pump
    on sd.date = pump.date
full join excluded_marginfi_volume as margin
    on sd.date = margin.date
full join orca_volume as orca
    on sd.date = orca.date
full join lifinity_volume as lifinity
    on sd.date = lifinity.date
group by 1
order by date asc
