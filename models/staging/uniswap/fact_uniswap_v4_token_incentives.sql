{{ config(materialized="table") }}


SELECT
    block_timestamp::date as date,
    sum(amount) as amount_native,
    sum(amount_usd) as amount
FROM ethereum_flipside.core.ez_token_transfers
WHERE from_address = lower('0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae') --MERKL Angle Distributor Contract
AND contract_address = lower('0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984') --UNI Token
AND block_timestamp::date > '2025-04-01'
GROUP BY 1