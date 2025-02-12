{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}
WITH contracts AS (
    SELECT
        *
    FROM {{ ref('dim_all_addresses_labeled_gold') }}
    WHERE
        chain = 'injective'
), last_day AS (
    SELECT
        contracts.address,
        contracts.name,
        metrics.app,
        metrics.txns,
        metrics.dau,
        metrics.gas,
        metrics.gas_usd,
        metrics.friendly_name,
        metrics.category
    FROM {{ ref('ez_injective_metrics_by_application') }} metrics
    LEFT join contracts 
        ON metrics.app = contracts.app
    where 
        contracts.address is not null 
        AND metrics.date >= dateadd(day, -1, current_date)
), first_day AS (
    select
        contracts.address,
        contracts.name,
        metrics.app,
        metrics.txns,
        metrics.dau,
        metrics.gas,
        metrics.gas_usd
    FROM {{ ref('ez_injective_metrics_by_application') }} metrics
    LEFT join contracts 
        ON metrics.app = contracts.app
    WHERE
        contracts.address IS NOT NULL
        AND metrics.date < dateadd(day, -1, current_date)
        AND metrics.date >= dateadd(day, -2, current_date)
)
select
    -- Dedup to pick one record per app
    MAX(last_day.address) to_address,
    MAX(last_day.txns) txns,
    MAX(last_day.gas) gas,
    MAX(last_day.gas_usd) gas_usd,
    MAX(last_day.dau) dau,
    MAX(first_day.txns) prev_txns,
    MAX(first_day.gas) prev_gas,
    MAX(first_day.gas_usd) prev_gas_usd,
    MAX(first_day.dau) prev_dau,
    MAX(last_day.name) name,
    last_day.app as namespace,
    MAX(last_day.friendly_name) friendly_name,
    MAX(last_day.category) category,
    'daily' as granularity
from last_day
left join first_day on lower(last_day.address) = lower(first_day.address)
group by
    namespace
