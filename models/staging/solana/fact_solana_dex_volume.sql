-- This query is used to calculate the daily volume of dex swaps on Solana.
-- It excludes marginfi flash loans and is used to calculate the daily volume of dex swaps on Solana.

{{ config(materialized="table", snowflake_warehouse="SOLANA") }}

with all_marginfi_flash_loans as (
    select *
    from solana_flipside.core.ez_events_decoded
    where program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' and event_type = 'lendingAccountStartFlashloan'
)

select 
    date_trunc('day', block_timestamp) as date,
    sum(
        case
            when swap_from_amount_usd is not null and swap_to_amount_usd is not null then least(swap_from_amount_usd, swap_to_amount_usd)
            when swap_from_amount_usd is not null then swap_from_amount_usd
            when swap_to_amount_usd is not null then swap_to_amount_usd
            else 0
        end
    ) as daily_volume_usd
from solana_flipside.defi.ez_dex_swaps
where date_trunc('day', block_timestamp) > '2024-03-02' and date_trunc('day', block_timestamp) <> '2025-03-03' and tx_id not in (select tx_id from all_marginfi_flash_loans)
group by date
order by date asc