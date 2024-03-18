{{ config(materialized="table") }}
select date, verified_contracts, chain
from {{ ref("fact_ethereum_verified_contracts") }}
