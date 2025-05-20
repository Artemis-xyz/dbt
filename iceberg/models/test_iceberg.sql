{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="TEST",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="EZ_TEST_METRICS",
    )
}}

SELECT
    *
FROM (
    VALUES (
        ('a'),
        ('b')
    )
) AS t(c1, c2)

