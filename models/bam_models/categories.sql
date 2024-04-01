{{ config(materialized="table") }}

with
    categories as (
        select distinct category, chain
        from {{ ref("all_chains_gas_dau_txns_by_category") }}
    )
select case when category = 'Unlabeled' then null else category end as category, chain
from categories
order by 2, 1
