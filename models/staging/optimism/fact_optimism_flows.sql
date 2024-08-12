{{
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM",
        unique_key=["chain", "date", "from_category"],
    )
}}

WITH category_flows as (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as date,
        from_category,
        to_category,
        sum(amount_usd) as amount_usd
    FROM {{ ref("fact_optimism_labeled_transfers") }}
    GROUP BY from_category, to_category, date
), application_flows as (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as date,
        from_category,
        to_app,
        max(to_friendly_name) as to_friendly_name,
        sum(amount_usd) as amount_usd
    FROM {{ ref("fact_optimism_labeled_transfers") }}
    GROUP BY from_category, to_app, date
)
SELECT
    'optimism' as chain,
    cf.date,
    cf.from_category,
    cf.to_category,
    cf.amount_usd as category_amount_usd,
    af.to_app,
    af.to_friendly_name,
    af.amount_usd as application_amount_usd
FROM category_flows cf
left JOIN application_flows af
ON cf.date = af.date AND cf.to_category = af.from_category
