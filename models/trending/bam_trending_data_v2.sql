{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_MD") }}

select *, 'arbitrum' as chain
from {{ ref("arb_trending_daily_v2") }}
union all
select *, 'arbitrum' as chain
from {{ ref("arb_trending_weekly_monthly_v2") }}
union all
select *, 'avalanche' as chain
from {{ ref("avax_trending_daily_v2") }}
union all
select *, 'avalanche' as chain
from {{ ref("avax_trending_weekly_monthly_v2") }}
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
from {{ ref("eth_trending_daily_v2") }}
union all
select *, 'ethereum' as chain
from {{ ref("eth_trending_weekly_monthly_v2") }}
union all
select *, 'near' as chain
from {{ ref("near_trending_daily_v2") }}
union all
select *, 'near' as chain
from {{ ref("near_trending_weekly_monthly_v2") }}
union all
select *, 'optimism' as chain
from {{ ref("opt_trending_daily_v2") }}
union all
select *, 'optimism' as chain
from {{ ref("opt_trending_weekly_monthly_v2") }}
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
