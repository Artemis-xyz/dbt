{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="UNISWAP",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="EZ_METRICS_BY_TOKEN",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'date, token'
        }) }}"
    )
}}

SELECT
    * EXCLUDE(DATE),
    DATE::TIMESTAMP_NTZ(6) AS DATE
FROM uniswap.prod_core.ez_metrics_by_token