{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="MORPHO",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="EZ_METRICS_BY_CHAIN",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'date, chain'
        }) }}"
    )
}}

SELECT
    * EXCLUDE(DATE),
    DATE::TIMESTAMP_NTZ(6) AS DATE
FROM morpho.prod_core.ez_metrics_by_chain