{{
    config(
        materialized="table"
    )
}}

SELECT * FROM {{ ref("dim_all_apps_silver") }}