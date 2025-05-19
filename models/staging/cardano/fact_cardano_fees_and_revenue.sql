{{ config(materialized="view") }}

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
