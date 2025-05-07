{{ config(materialized="table") }}

select
    date,
    coalesce(sum(incentives), 0) as total_incentives
from {{ ref("uniswap_token_incentives") }}
group by 1