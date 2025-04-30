{{ config( materialized="table") }}

select
    *
from {{ ref("fact_raydium_stablecoin_apy") }}
union all
select
    *
from {{ ref("fact_kamino_stablecoin_apy") }}