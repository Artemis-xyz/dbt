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
        ON p.contract_address = e.contract_address
        and p.date = e.date
    WHERE p.symbol ilike '%ETH%'
    GROUP BY e.date
    ORDER BY e.date desc
)
select
    date,
    amount_native as num_restaked_eth,
    amount as amount_restaked_usd,
    amount_native - lag(amount_native) over (order by date) as num_restaked_eth_net_change,
    amount - lag(amount) over (order by date) as amount_restaked_usd_net_change
from processed_data