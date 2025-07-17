{{
    config(
        materialized="table",
        snowflake_warehouse="DIMO",
    )
}}

SELECT
    block_timestamp::date as date,
    SUM(amount) AS daily_airdrop_amount_native,
    SUM(amount_usd) AS daily_airdrop_amount
FROM
    {{ source("POLYGON_FLIPSIDE", "ez_token_transfers") }}
WHERE
    from_address = lower('0x4561D7Cd96d3acbd040ee526Ec1bB3405D7bbD1f') 
    AND contract_address = lower('0xe261d618a959afffd53168cd07d12e37b26761db')
GROUP BY
    1
