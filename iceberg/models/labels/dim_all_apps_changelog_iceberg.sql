{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="LABELS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="dim_all_apps_changelog",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'artemis_application_id'
        }) }}"
    )
}}

SELECT
    * EXCLUDE (last_updated_timestamp),
    last_updated_timestamp::TIMESTAMP_NTZ(6) AS last_updated_timestamp
FROM PC_DBT_DB.PROD.DIM_ALL_APPS_CHANGELOG
ORDER BY artemis_application_id