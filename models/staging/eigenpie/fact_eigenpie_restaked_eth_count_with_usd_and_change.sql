{{ config(materialized="table") }}

WITH prices AS (
    {{ get_multiple_coingecko_price_with_latest("ethereum") }}
)
, processed_data as (
    SELECT
        e.date,
        sum(e.amount_native) as amount_native,
        sum(e.amount_native * p.price) as amount,
    FROM {{ref('fact_eigenpie_restaked_eth_count')}} e
    LEFT JOIN prices p
        ON lower(p.contract_address) = lower(e.contract_address)
        and p.date = e.date
    WHERE p.symbol ilike '%ETH%'
    GROUP BY e.date
    ORDER BY e.date desc
)
select
    date,
    'ethereum' as chain,
    amount_native as num_restaked_eth,
    amount as amount_restaked_usd,
    coalesce(amount_native - lag(amount_native) over (order by date), 0) as num_restaked_eth_net_change,
    coalesce(amount - lag(amount) over (order by date), 0) as amount_restaked_usd_net_change
from processed_data