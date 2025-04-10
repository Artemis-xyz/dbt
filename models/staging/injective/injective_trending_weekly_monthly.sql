{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}
WITH contracts AS (
    SELECT
        *
    FROM {{ ref('dim_contracts_gold') }}
    WHERE
        chain = 'injective'
), last_week AS (
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
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application_v2') }} metrics
    LEFT JOIN contracts
        ON metrics.app = contracts.app
    where 
        contracts.address is not null 
        AND metrics.date >= dateadd(month, -7, current_date)
), first_week AS (
    select
        contracts.address,
        contracts.name,
        metrics.app,
        metrics.txns,
        metrics.dau,
        metrics.gas,
        metrics.gas_usd,
        metrics.friendly_name,
        metrics.category
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application_v2') }} metrics
    LEFT JOIN contracts
        ON metrics.app = contracts.app
    WHERE
        contracts.address is not null 
        AND metrics.date < dateadd(day, -7, current_date)
        AND metrics.date >= dateadd(day, -14, current_date)
), trending_week AS (
    select
        last_week.address,
        last_week.txns,
        last_week.gas,
        last_week.gas_usd,
        last_week.dau,
        first_week.txns prev_txns,
        first_week.gas prev_gas,
        first_week.gas_usd prev_gas_usd,
        first_week.dau prev_dau,
        last_week.name,
        last_week.app namespace,
        last_week.friendly_name,
        last_week.category,
        'weekly' as granularity
    from last_week
    left join first_week on lower(last_week.address) = lower(first_week.address)
), last_month AS (
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
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application_v2') }} metrics
    LEFT JOIN contracts
        ON metrics.app = contracts.app
    where 
        contracts.address is not null 
        AND metrics.date >= dateadd(month, -30, current_date)
), first_month AS (
    select
        contracts.address,
        contracts.name,
        metrics.app,
        metrics.txns,
        metrics.dau,
        metrics.gas,
        metrics.gas_usd,
        metrics.friendly_name,
        metrics.category
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application_v2') }} metrics
    LEFT JOIN contracts
        ON metrics.app = contracts.app
    where 
        contracts.address is not null 
        AND metrics.date < dateadd(day, -30, current_date)
        AND metrics.date >= dateadd(day, -60, current_date)
), trending_month as (
    select
        last_month.address to_address,
        last_month.txns,
        last_month.gas,
        last_month.gas_usd,
        last_month.dau,
        first_month.txns prev_txns,
        first_month.gas prev_gas,
        first_month.gas_usd prev_gas_usd,
        first_month.dau prev_dau,
        last_month.name,
        last_month.app as namespace,
        last_month.friendly_name,
        last_month.category,
        'monthly' as granularity
    from last_month
    left join first_month on lower(last_month.address) = lower(first_month.address)
), unioned AS (
    select *
    from trending_week
    union
    select *
    from trending_month
)
-- Dedup to pick one record per app
SELECT
    MAX(address) to_address,
    MAX(txns) txns,
    MAX(gas) gas,
    MAX(gas_usd) gas_usd,
    MAX(dau) dau,
    MAX(prev_txns) prev_txns,
    MAX(prev_gas) prev_gas,
    MAX(prev_gas_usd) prev_gas_usd,
    MAX(prev_dau) prev_dau,
    MAX(name) name,
    namespace,
    MAX(friendly_name) friendly_name,
    MAX(category) category,
    granularity
FROM unioned
group by
    namespace, granularity
