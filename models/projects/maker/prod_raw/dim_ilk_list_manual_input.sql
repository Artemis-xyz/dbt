{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_ilk_list_manual_input"
    )
}}

SELECT * FROM (VALUES
    ('RWA009-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA007-A', NULL, NULL, 12310, 31172, NULL),
    ('RWA015-A', NULL, NULL, 12310, 31172, NULL),
    ('RWA014-A', NULL, NULL, 12310, 31180, NULL)
) AS t(ilk, begin_dt, end_dt, asset_code, equity_code, apr)