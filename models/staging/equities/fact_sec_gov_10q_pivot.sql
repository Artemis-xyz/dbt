{{ config(materialized="table") }}

WITH RECURSIVE path_builder AS (
    SELECT 
        cik,
        adsh,
        company_name,
        metric_name, 
        metric_value,
        time_period,
        date,
        ARRAY_CONSTRUCT(metric_name) AS col_path
    FROM {{ ref("fact_sec_gov_10q") }}
    WHERE parent_metric IS NULL

    UNION ALL

    SELECT 
        child.cik,
        child.adsh,
        child.company_name,
        child.metric_name, 
        child.metric_value, 
        child.time_period,
        child.date,
        ARRAY_APPEND(parent.col_path, child.metric_name) AS col_path
    FROM {{ ref("fact_sec_gov_10q") }} AS child
    JOIN path_builder AS parent
    ON child.parent_metric = parent.metric_name AND child.adsh = parent.adsh AND child.time_period = parent.time_period
)

SELECT * FROM path_builder ORDER BY metric_name