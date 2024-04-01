{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}

{{
    fact_daily_uniswap_v3_fork_tvl(
        "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7", "bsc", "uniswap_v3"
    )
}}

-- (
-- "0x24db544cdfe08d29b1a4c2f8fd4566fba664db24",
-- "0x864d8a56c7db0c5ef6111cc2398fc97d27f7836a",
-- "0x80af1b760a5fe837465f58f92b2723809cb9c7cd",
-- "0x582a56fa93c9f8c0a9e1a8117555e6e2749ba99d",
-- "0xd0ef7c7c809ba57f00a357ab2cf9242eba1f3849",
-- "0x2938d33b8983a98d81f68672f1338dcda57ffc57",
-- "0xcdf1c1ee77d47d1423b8ab7ef8acf962c57c9e97",
-- "0xa97a3538bc566bb7620d937f675f4656d69956cb",
-- "0xf8f45d8f8ee6721891f3ca153c790361cbdead78",
-- ),

