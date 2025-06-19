{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
    )
}}


SELECT
    ds.date,
    1e9 as max_supply,
    0 as uncreated_tokens,
    1e9 as total_supply,
    treasury_native,
    total_supply - treasury_native as issued_supply,
    ((400 * 1e6) - coalesce(v.total_insider_vested, 400e6)) as unvested_insider_tokens,
    issued_supply - unvested_insider_tokens as circulating_supply
FROM dim_date_spine ds
LEFT JOIN uniswap.prod_raw.fact_uniswap_treasury_by_token t USING(DATE)
LEFT JOIN fact_uniswap_insider_vesting v USING(DATE)
WHERE ds.date between '2020-09-15' AND to_date(sysdate())
AND t.token = 'UNI';
