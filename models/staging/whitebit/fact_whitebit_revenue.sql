{{config(
    materialized="table"
)}}

SELECT
    date(block_timestamp) AS date,
    tx_hash,    
    -- Revenue is generated from buybacks and burns of WBT by Whitebit. 
    amount AS revenue_native, 
    amount_usd AS revenue
FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }}
WHERE 1=1
    AND LOWER(to_address) = LOWER('0x0000000000000000000000000000000000000000')
    AND LOWER(contract_address) = LOWER('0x925206b8a707096ed26ae47c84747fe0bb734f59')
    AND LOWER(tx_hash) != LOWER('0xc36d68c68f87a9704d3121c1c5ddc0f7f2cdb148aeba6c6fab5e862fd7eaa1b8')
    AND LOWER(tx_hash) != LOWER('0x86e272c09b3f17d0b1b9349c2361957f647dbe0dd8c89cec7f5e7febc12c8ad2')