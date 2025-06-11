{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="LABELS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="fact_daily_bam_datahub_v2",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'chain, date'
        }) }}"
    )
}}

SELECT
    * EXCLUDE (date),
    date::TIMESTAMP_NTZ(6) AS date
FROM PC_DBT_DB.PROD.FACT_DAILY_BAM_DATAHUB_V2