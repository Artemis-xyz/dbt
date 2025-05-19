{{ config(
    materialized='incremental',
    unique_key='date',
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
{% if is_incremental() %}
  and date > (select coalesce(max(date), '1900-01-01') from {{ this }})
{% endif %}
order by date desc