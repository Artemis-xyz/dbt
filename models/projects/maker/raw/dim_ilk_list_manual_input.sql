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
    ('RWA014-A', NULL, NULL, 12310, 31180, NULL),
    ('PSM-GUSD-A', NULL, '2022-10-31', 13410, NULL, NULL), --could make rate 0 as well.
    ('PSM-GUSD-A', '2022-11-01', NULL, 13411, 31180, 0.0125),
    ('RWA001-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA002-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA003-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA004-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA005-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA006-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA007-A', NULL, NULL, 12320, 31172, NULL),
    ('RWA008-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA009-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA014-A', NULL, NULL, 13411, 31180, NULL),
    ('RWA015-A', NULL, NULL, 12320, 31172, NULL),
    ('UNIV2DAIUSDC-A', NULL, NULL, 11140, 31140, NULL), --need to list all UNIV2% LP that are stable LPs, all else assumed volatile
    ('UNIV2DAIUSDT-A', NULL, NULL, 11140, 31140, NULL)
) AS t(ilk, begin_dt, end_dt, asset_code, equity_code, apr)