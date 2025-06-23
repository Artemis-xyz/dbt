{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="DATAHUBS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="fact_app_retention_datahub",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'chain, app'
        }) }}"
    )
}}

SELECT
    * EXCLUDE(COHORT_MONTH),
    COHORT_MONTH::TIMESTAMP_NTZ(6) AS COHORT_MONTH
FROM pc_dbt_db.prod.fact_app_retention_datahub