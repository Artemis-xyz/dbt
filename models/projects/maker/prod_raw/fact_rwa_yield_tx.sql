{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_rwa_yield_tx"
    )
}}

SELECT DISTINCT
    tx_hash,
    CASE 
        WHEN usr = '0x6c6d4be2223b5d202263515351034861dd9afdb6' THEN 'RWA009-A'
        WHEN usr = '0xef1b095f700be471981aae025f92b03091c3ad47' THEN 'RWA007-A'
        WHEN usr = '0x71ec6d5ee95b12062139311ca1fe8fd698cbe0cf' THEN 'RWA014-A'
        WHEN usr = lower('0xc27C3D3130563C1171feCC4F76C217Db603997cf') THEN 'RWA015-A'
    END AS ilk
FROM {{ ref('fact_dai_burn') }}
WHERE usr IN ('0x6c6d4be2223b5d202263515351034861dd9afdb6', '0xef1b095f700be471981aae025f92b03091c3ad47', '0x71ec6d5ee95b12062139311ca1fe8fd698cbe0cf', lower('0xc27C3D3130563C1171feCC4F76C217Db603997cf')) 