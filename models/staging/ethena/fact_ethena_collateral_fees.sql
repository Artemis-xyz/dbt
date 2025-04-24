SELECT
    DATE(block_timestamp) AS date,
    SUM(
        TRY_TO_NUMBER(NULLIF(DECODED_LOG:usde_amount :: STRING, '')) / 1e21
    ) AS collateral_fee
FROM
    ethereum_flipside.core.ez_decoded_event_logs
WHERE
    contract_address = '0x2cc440b721d2cafd6d64908d6d8c4acc57f8afc3'
    AND event_name = 'Mint'
GROUP BY
    DATE(block_timestamp)
ORDER BY
    DATE(block_timestamp) DESC