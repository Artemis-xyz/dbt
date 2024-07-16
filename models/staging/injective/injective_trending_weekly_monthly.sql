{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}
WITH contracts AS (
    SELECT
        *
    FROM {{ ref('dim_contracts_gold') }}
    WHERE
        chain = 'injective'
), dates AS (
    SELECT 
        DATEADD(day, -7, MAX(date))  AS last_week,
        DATEADD(day, -14, MAX(date))  AS first_week,
        DATEADD(day, -30, MAX(date))  AS last_month,
        DATEADD(day, -60, MAX(date))  AS first_month
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}
), last_week AS (
    SELECT
        app,
        txns,
        dau,
        gas,
        gas_usd,
        friendly_name,
        category
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}, dates
    where 
        app is not null 
        and date >= dates.last_week
), first_week AS (
    select
        app,
        txns,
        dau,
        gas,
        gas_usd
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}, dates
    where
        app is not null
        and date < dates.last_week
        and date >= dates.first_week
), trending_week AS (
    select
        contracts.address AS to_address,
        last_week.txns txns,
        last_week.gas gas,
        last_week.gas_usd gas_usd,
        last_week.dau dau,
        first_week.txns prev_txns,
        first_week.gas prev_gas,
        first_week.gas_usd prev_gas_usd,
        first_week.dau prev_dau,
        last_week.friendly_name,
        last_week.category,
        'weekly' as granularity
    from last_week
    left join first_week on lower(last_week.app) = lower(first_week.app)
    inner join contracts 
        ON last_week.app = contracts.app
), last_month AS (
    SELECT
        app,
        txns,
        dau,
        gas,
        gas_usd,
        friendly_name,
        category
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}, dates
    where 
        app is not null 
        and date >= dates.last_month
), first_month AS (
    select
        app,
        txns,
        dau,
        gas,
        gas_usd
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}, dates
    where
        app is not null
        and date < dates.last_month
        and date >= dates.first_month
), trending_month as (
    select
        contracts.address AS to_address,
        last_month.txns,
        last_month.gas,
        last_month.gas_usd,
        last_month.dau,
        first_month.txns prev_txns,
        first_month.gas prev_gas,
        first_month.gas_usd prev_gas_usd,
        first_month.dau prev_dau,
        last_month.friendly_name,
        last_month.category,
        'monthly' as granularity
    from last_month
    left join
        first_month on lower(last_month.app) = lower(first_month.app)
    inner join contracts
        ON last_month.app = contracts.app
)
select *
from trending_week
union
select *
from trending_month
