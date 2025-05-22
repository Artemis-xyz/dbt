{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="BALANCER",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="EZ_METRICS_BY_POOL",
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'date, pool_address'
        }) }}"
    )
}}

SELECT
    * EXCLUDE(DATE),
    DATE::TIMESTAMP_NTZ(6) AS DATE
FROM balancer.prod_core.ez_metrics_by_pool