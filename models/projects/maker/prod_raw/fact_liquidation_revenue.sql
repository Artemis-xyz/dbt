{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_liquidation_revenue"
    )
}}

SELECT
    date(block_timestamp) as date,
    SUM(rad) AS daily_sum
FROM ethereum_flipside.maker.fact_vat_move
WHERE dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    AND src_address NOT IN (SELECT contract_address FROM maker.prod_raw.dim_contracts)
    AND src_address NOT IN ('0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a'
        , '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2') -- aave v2 d3m, compound v2 d3m
    AND tx_hash NOT IN (SELECT tx_hash FROM maker.prod_raw.dim_liquidation_excluded_tx) -- Exclude Flop income (coming directly from users wallets)
    AND tx_hash NOT IN (SELECT tx_hash FROM maker.prod_raw.dim_team_dai_burns_tx)
    AND tx_hash NOT IN (SELECT tx_hash FROM maker.prod_raw.dim_psm_yield_tx)
    AND tx_hash NOT IN (SELECT tx_hash FROM maker.prod_raw.dim_rwa_yield_tx)
GROUP BY date(block_timestamp)