{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}
WITH contracts AS (
    SELECT
        *
    FROM {{ ref('dim_contracts_gold') }}
    WHERE
        chain = 'injective'
), dates AS (
    SELECT 
        DATEADD(day, -1, MAX(date))  AS last_date,
        DATEADD(day, -2, MAX(date))  AS first_date
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}
), last_day AS (
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
        and date >= dates.last_date
), first_day AS (
    select
        app,
        txns,
        dau,
        gas,
        gas_usd
    FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}, dates
    where
        app is not null
        and date < dates.last_date
        and date >= dates.first_date
)
select
    contracts.address AS to_address,
    last_day.app,
    last_day.txns txns,
    last_day.gas gas,
    last_day.gas_usd gas_usd,
    last_day.dau dau,
    first_day.txns prev_txns,
    first_day.gas prev_gas,
    first_day.gas_usd prev_gas_usd,
    first_day.dau prev_dau,
    last_day.friendly_name,
    last_day.category,
    'daily' as granularity
from last_day
left join first_day on lower(last_day.app) = lower(first_day.app)
inner join contracts 
    ON last_day.app = contracts.app
