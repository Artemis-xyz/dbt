{{ config(
    materialized='incremental',
    unique_key='date',
    snowflake_warehouse='CARDANO'
) }}

with 
    max_date as (
        select coalesce(max(date), '1900-01-01') as max_date from {{ this }}
    ),
    -- Get transaction timestamps
    tx_times as (
        select
            tx_hash,
            block_time,
            epoch_no,
            slot_no,
            txidx
        from {{ ref('fact_cardano_tx') }}
        where block_time < current_date()
        {% if is_incremental() %}
          and block_time >= dateadd(day, 1, (select max_date from max_date))
        {% endif %}
    ),
    -- Get input addresses from tx_in_out
    input_addresses as (
        select
            date_trunc('day', t.block_time) as date,
            prev_out.value:out_address::string as address
        from {{ ref('fact_cardano_tx_in_out') }} txio
        inner join tx_times t 
            on txio.epoch_no = t.epoch_no 
            and txio.slot_no = t.slot_no 
            and txio.txidx = t.txidx
        , lateral flatten(input => PARSE_JSON(txio.inputs)) inp
        inner join {{ ref('fact_cardano_tx_in_out') }} prev_txio
            on prev_txio.slot_no = inp.value:in_slot_no::integer
            and prev_txio.txidx = inp.value:in_txidx::integer
        , lateral flatten(input => PARSE_JSON(prev_txio.outputs)) prev_out
        where prev_out.value:out_idx::integer = inp.value:in_idx::integer
          and prev_out.value:out_address is not null
        {% if is_incremental() %}
          and date_trunc('day', t.block_time) > (select max_date from max_date)
        {% endif %}
    ),
    -- Get output addresses from tx_in_out
    output_addresses as (
        select
            date_trunc('day', t.block_time) as date,
            f.value:out_address::string as address
        from {{ ref('fact_cardano_tx_in_out') }} txio
        inner join tx_times t 
            on txio.epoch_no = t.epoch_no 
            and txio.slot_no = t.slot_no 
            and txio.txidx = t.txidx
        , lateral flatten(input => PARSE_JSON(txio.outputs)) f
        where f.value:out_address is not null
        {% if is_incremental() %}
          and date_trunc('day', t.block_time) > (select max_date from max_date)
        {% endif %}
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

order by date desc