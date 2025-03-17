{{ config(materialized="table") }}

select 
    date
    , initial_contributors_supply
    , investors_supply
    , airdrop_supply
    , future_community_initiatives_supply
    , ecosystem_research_and_development_supply
    , total_supply
    , sum(total_supply) over (order by date asc rows between unbounded preceding and current row) as current_circulating_supply
from {{ source('MANUAL_STATIC_TABLES', 'berachain_supply_data') }}
