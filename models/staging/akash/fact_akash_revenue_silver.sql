
select
    akt.date,
    (p.price * akt.compute_fees_native / 1e6 * 0.04)
    + (usd.compute_fees_usdc / 1e6 * 0.2) as revenue,
from {{ ref("fact_akash_compute_fees_native_silver")}} akt
left join
    ({{ get_coingecko_price_with_latest("akash-network") }}) p on p.date = akt.date
left join {{ref("fact_akash_compute_fees_usdc_silver")}} usd on akt.date = usd.date
