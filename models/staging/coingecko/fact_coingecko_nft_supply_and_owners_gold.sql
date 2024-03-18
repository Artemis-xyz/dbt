{{ config(materialized="table") }}

select date, coingecko_nft_id, nft_number_of_unique_addresses, nft_total_supply
from {{ ref("fact_coingecko_nft_supply_and_owners") }}
