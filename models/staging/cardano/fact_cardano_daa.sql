{{ config(
    materialized='table',
    snowflake_warehouse='CARDANO'
) }}

with 
    -- Get transaction timestamps
    tx_times as (
        select
            tx_hash,
            block_time
        from {{ ref('fact_cardano_transactions') }}
    ),
    -- Get input addresses from tx_in_out
    input_addresses as (
        select
            date_trunc('day', t.block_time) as date,
            f.value:address::string as address
        from {{ ref('fact_cardano_tx_in_out') }} txio
        inner join tx_times t 
            on txio.epoch_no = t.epoch_no 
            and txio.slot_no = t.slot_no 
            and txio.txidx = t.txidx,
        lateral flatten(input => txio.inputs) f
        where f.value:address is not null
    ),
    -- Get output addresses from tx_in_out
    output_addresses as (
        select
            date_trunc('day', t.block_time) as date,
            f.value:address::string as address
        from {{ ref('fact_cardano_tx_in_out') }} txio
        inner join tx_times t 
            on txio.epoch_no = t.epoch_no 
            and txio.slot_no = t.slot_no 
            and txio.txidx = t.txidx,
        lateral flatten(input => txio.outputs) f
        where f.value:address is not null
    ),
    -- Combine unique addresses for each day
    unique_daily_addresses as (
        select date, address
        from input_addresses
        union distinct
        select date, address
        from output_addresses
    ),
    -- Calculate daily active addresses (DAA)
    daily_active_addresses as (
        select
            date,
            count(distinct address) as daa,
            'cardano' as chain
        from unique_daily_addresses
        group by 1
    )

select 
    date,
    daa,
    chain
from daily_active_addresses
where date < current_date()
order by date desc