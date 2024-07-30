{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_opex"
    )
}}

WITH opex_preunion AS (
    SELECT 
        mints.block_timestamp AS ts,
        mints.tx_hash AS hash,
        CASE
            WHEN dao_wallet.code IN ('GELATO', 'KEEP3R', 'CHAINLINK', 'TECHOPS') THEN 31710 --keeper maintenance expenses
            WHEN dao_wallet.code = 'GAS' THEN 31630 -- oracle gas expenses
            WHEN dao_wallet.code IS NOT NULL THEN 31720 --workforce expenses
            ELSE 31740 --direct opex - when a suck operation is used to directly transfer DAI to a third party
        END AS equity_code,
        mints.wad / POW(10, 18) AS expense
    FROM {{ ref('fact_dai_mint') }} mints
    JOIN {{ ref('fact_opex_suck_hashes') }} opex
        ON mints.tx_hash = opex.tx_hash
    LEFT JOIN {{ ref('dim_dao_wallet') }} dao_wallet
        ON mints.usr = dao_wallet.wallet_address
    LEFT JOIN ethereum_flipside.maker.fact_vat_frob AS frobs
        ON mints.tx_hash = frobs.tx_hash
        AND mints.wad::number/1e18 = frobs.dart
    WHERE frobs.tx_hash IS NULL --filtering out draws from psm that happened in the same tx as expenses
)

SELECT
    ts,
    hash,
    equity_code AS code,
    -CAST(expense AS DOUBLE) AS value --reduced equity
FROM opex_preunion

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    expense AS value --increased liability
FROM opex_preunion