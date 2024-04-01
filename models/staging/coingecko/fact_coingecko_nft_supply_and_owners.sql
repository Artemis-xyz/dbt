{{ config(materialized="view") }}
select
    date,
    coingecko_nft_id,
    max(nft_number_of_unique_addresses) as nft_number_of_unique_addresses,
    max(nft_total_supply) as nft_total_supply
from {{ ref("fact_coingecko_nft_metadata") }}
where nft_number_of_unique_addresses is not null and nft_total_supply is not null
group by date, coingecko_nft_id
