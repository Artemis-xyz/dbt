{{ config(
    materialized='incremental',
    unique_key='date',
    snowflake_warehouse='CARDANO'
) }}

with
    tx_daily as (
        select
            date_trunc('day', block_time) as date,
            sum(fee) as total_fee_lovelace
        from {{ ref('fact_cardano_tx') }}
        where block_time is not null
        group by 1
    ),
    cardano_prices as ({{ get_coingecko_price_with_latest('cardano') }} )

select
    tx_daily.date,
    'cardano' as chain,
    total_fee_lovelace / 1e6 as gas, -- ADA
    (total_fee_lovelace / 1e6) * coalesce(cardano_prices.price, 0) as gas_usd,
    0 as revenue
from tx_daily
left join cardano_prices on tx_daily.date = cardano_prices.date
where tx_daily.date < current_date()
{% if is_incremental() %}
  and tx_daily.date > (select coalesce(max(date), '1900-01-01') from {{ this }})
{% endif %}
