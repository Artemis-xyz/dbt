{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_treasury_flows"
    )
}}

WITH treasury_flows_preunioned AS (
    SELECT 
        evt.block_timestamp AS ts,
        evt.tx_hash AS hash,
        t.token,
        SUM(evt.RAW_AMOUNT_PRECISE / POW(10, t.decimals)) AS value
    FROM ethereum_flipside.core.ez_token_transfers evt
    JOIN {{ ref('dim_treasury_erc20s') }} t
        ON evt.contract_address = t.contract_address
    WHERE evt.to_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    GROUP BY evt.block_timestamp, evt.tx_hash, t.token

    UNION ALL

    SELECT 
        evt.block_timestamp AS ts,
        evt.tx_hash AS hash,
        t.token,
        -SUM(evt.RAW_AMOUNT_PRECISE / POW(10, t.decimals)) AS value
    FROM ethereum_flipside.core.ez_token_transfers evt
    JOIN {{ ref('dim_treasury_erc20s') }} t
        ON evt.contract_address = t.contract_address
    WHERE evt.from_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    AND evt.to_address != '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    GROUP BY evt.block_timestamp, evt.tx_hash, t.token
)

SELECT
    ts,
    hash,
    33110 AS code,
    value, --increased equity
    token
FROM treasury_flows_preunioned

UNION ALL

SELECT
    ts,
    hash,
    14620 AS code,
    value, --increased assets
    token
FROM treasury_flows_preunioned