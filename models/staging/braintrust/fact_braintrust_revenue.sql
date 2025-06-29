{{
    config(
        materialized="table",
        snowflake_warehouse="BRAINTRUST",
    )
}}

SELECT
    block_timestamp::date as date,
    sum(amount_usd) as revenue
FROM
    {{ source("ETHEREUM_FLIPSIDE", "ez_token_transfers") }}
WHERE lower(contract_address) = lower('0x799ebfABE77a6E34311eeEe9825190B9ECe32824')
AND to_address = lower('0xb6f1F016175588a049fDA12491cF3686De33990B')
GROUP BY 1