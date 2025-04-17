{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_team_dai_burns"
    )
}}

WITH team_dai_burns_preunioned AS (
    SELECT vat.block_timestamp AS ts,
           vat.tx_hash AS hash,
           tx.is_keeper,
           SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    JOIN {{ ref('fact_team_dai_burns_tx') }} tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY vat.block_timestamp, vat.tx_hash, tx.is_keeper
)

SELECT ts,
       hash,
       (CASE WHEN is_keeper THEN 31710 ELSE 31730 END) AS code,
       value -- increased equity
FROM team_dai_burns_preunioned

UNION ALL

SELECT ts,
       hash,
       21120 AS code,
       -value AS value -- decreased liability
FROM team_dai_burns_preunioned