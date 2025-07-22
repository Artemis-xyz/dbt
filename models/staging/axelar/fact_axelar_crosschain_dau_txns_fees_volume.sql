{{ config(
    materialized="table",
    snowflake_warehouse="AXELAR"
) }}

WITH
    max_extraction AS (
        SELECT MAX(extraction_date) AS max_date
        FROM {{ source("PROD_LANDING", "raw_axelar_dau_txns_volume_fees") }}
    )
    , latest_data AS (
        SELECT 
            DATE(value:timestamp) AS date
            , NULL AS bridge_daa
            , NULL AS fees
            , value:num_txs::number AS bridge_txns
            , value:volume::number AS volume
        FROM LANDING_DATABASE.PROD_LANDING.raw_axelar_dau_txns_volume_fees,
        LATERAL FLATTEN(input => parse_json(source_json):data)
        WHERE DATE(extraction_date) = '2025-07-22' AND DATE(value:timestamp) > '2025-04-28'
        AND value:timestamp <> 0

        UNION ALL 

        SELECT 
            DATE(value:timestamp) AS date
            , value:users::number AS bridge_daa
            , value:fee::number AS fees
            , value:num_txs::number AS bridge_txns
            , value:volume::number AS volume
        FROM LANDING_DATABASE.PROD_LANDING.raw_axelar_dau_txns_volume_fees,
        LATERAL FLATTEN(input => parse_json(source_json):data)
        WHERE DATE(extraction_date) = '2025-04-28' AND DATE(value:timestamp) <= '2025-04-28'
        AND value:timestamp <> 0
    )

SELECT
    date
    , bridge_daa
    , fees
    , bridge_txns
    , volume
    , 'axelar' AS chain
    , 'axelar' AS app
FROM latest_data

