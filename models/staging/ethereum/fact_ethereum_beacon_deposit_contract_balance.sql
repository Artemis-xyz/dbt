{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

SELECT
    block_timestamp::date as date,
    max_by(balance_token, block_timestamp) as balance_native
FROM
    pc_dbt_db.prod.fact_ethereum_address_balances_by_token
WHERE
    1 = 1
    AND contract_address = 'native_token'
    AND address = lower('0x00000000219ab540356cbb839cbe05303d7705fa')
GROUP BY
    1