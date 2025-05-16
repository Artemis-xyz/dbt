{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_MD") }}

select *, 'arbitrum' as chain
from {{ ref("arbitrum_trending_daily_v2") }}
union all
select *, 'arbitrum' as chain
from {{ ref("arbitrum_trending_weekly_monthly_v2") }}
union all
select *, 'avalanche' as chain
from {{ ref("avalanche_trending_daily_v2") }}
union all
select *, 'avalanche' as chain
from {{ ref("avalanche_trending_weekly_monthly_v2") }}
union all
select *, 'base' as chain
from {{ ref("base_trending_daily_v2") }}
union all
select *, 'base' as chain
from {{ ref("base_trending_weekly_monthly_v2") }}
union all
select *, 'bsc' as chain
from {{ ref("bsc_trending_daily_v2") }}
union all
select *, 'bsc' as chain
from {{ ref("bsc_trending_weekly_monthly_v2") }}
union all
select *, 'ethereum' as chain
from {{ ref("ethereum_trending_daily_v2") }}
union all
select *, 'ethereum' as chain
from {{ ref("ethereum_trending_weekly_monthly_v2") }}
union all
select *, 'near' as chain
from {{ ref("near_trending_daily_v2") }}
union all
select *, 'near' as chain
from {{ ref("near_trending_weekly_monthly_v2") }}
union all
select *, 'optimism' as chain
from {{ ref("optimism_trending_daily_v2") }}
union all
select *, 'optimism' as chain
from {{ ref("optimism_trending_weekly_monthly_v2") }}
union all
select *, 'polygon' as chain
from {{ ref("polygon_trending_daily_v2") }}
union all
select *, 'polygon' as chain
from {{ ref("polygon_trending_weekly_monthly_v2") }}
union all
select *, 'sei' as chain
from {{ ref("sei_trending_daily_v2") }}
union all
select *, 'sei' as chain
from {{ ref("sei_trending_weekly_monthly_v2") }}
union all
select *, 'solana' as chain
from {{ ref("solana_trending_daily_v2") }}
union all
select *, 'solana' as chain
from {{ ref("solana_trending_weekly_monthly_v2") }}
union all
select *, 'stellar' as chain
from {{ ref("stellar_trending_daily_v2") }}
union all
select *, 'stellar' as chain
from {{ ref("stellar_trending_weekly_monthly_v2") }}
union all
select *, 'sui' as chain
from {{ ref("sui_trending_daily_v2") }}
union all
select *, 'sui' as chain
from {{ ref("sui_trending_weekly_monthly_v2") }}
union all
select *, 'tron' as chain
from {{ ref("tron_trending_daily_v2") }}
union all
select *, 'tron' as chain
from {{ ref("tron_trending_weekly_monthly_v2") }}
union all
select *, 'injective' as chain
from {{ ref("injective_trending_daily_v2") }}
union all
select *, 'injective' as chain
from {{ ref("injective_trending_weekly_monthly_v2") }}
union all
select *, 'mantle' as chain
from {{ ref("mantle_trending_daily_v2") }}
union all
select *, 'mantle' as chain
from {{ ref("mantle_trending_weekly_monthly_v2") }}
union all
select *, 'celo' as chain
from {{ ref("celo_trending_daily_v2") }}
union all
select *, 'celo' as chain
from {{ ref("celo_trending_weekly_monthly_v2") }}
