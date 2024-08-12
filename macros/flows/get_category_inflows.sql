{% macro get_category_inflows(chain) %}

WITH application_outflows as (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as date,
        from_app,
        max(from_friendly_name) as from_friendly_name,
        to_category,
        sum(amount_usd) as amount_usd
    FROM {{ ref("fact_" ~ chain ~ "_labeled_transfers") }}
    {% if is_incremental() %} 
        where block_timestamp >= (
            select dateadd('day', -3, max(date))
            from {{ this }}
        )
    {% endif %}
    GROUP BY from_app, to_category, date
), category_outflows as (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as date,
        from_category,
        to_category,
        max(to_friendly_name) as to_friendly_name,
        sum(amount_usd) as amount_usd
    FROM {{ ref("fact_" ~ chain ~ "_labeled_transfers") }}
    {% if is_incremental() %} 
        where block_timestamp >= (
            select dateadd('day', -3, max(date))
            from {{ this }}
        )
    {% endif %}
    GROUP BY from_category, to_category, date
)
SELECT
    'optimism' as chain,
    af.date,
    af.from_app,
    af.from_friendly_name,
    af.amount_usd as application_amount_usd,
    af.to_category,
    cf.amount_usd as category_amount_usd,
    cf.to_category as category
FROM application_outflows af
left JOIN category_outflows cf
ON af.date = cf.date AND af.to_category = cf.from_category

{% endmacro %}
