{{ config(
    materialized='table',
    snowflake_warehouse='CARDANO'
) }}

with daily_transactions as (
    select 
        date_trunc('day', block_time) as date,
        count(distinct tx_hash) as txns,
        'cardano' as chain
    from {{ ref('fact_cardano_tx') }}
    group by 1
)

select 
    date,
    txns,
    chain
from daily_transactions
where date < current_date()
order by date desc