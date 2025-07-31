{{
    config(
        materialized="incremental",
        snowflake_warehouse="RENDER",
        unique_key=["tx_id", "index", "inner_index"]    
    )
}}

SELECT 
    tr.block_timestamp,
    tr.tx_id,
    tr.index,
    tr.inner_index,
    tr.burn_amount / pow(10, p.decimals) as amount_native,
    p.price,
    amount_native * p.price as amount
FROM 
    solana_flipside.defi.fact_token_burn_actions tr
LEFT JOIN 
solana_flipside.price.ez_prices_hourly p ON tr.mint = p.token_address and p.hour = DATE_TRUNC('hour',block_timestamp)
WHERE 
    tr.mint = 'rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof'
    AND tr.block_timestamp >=  '2023-10-01' --YYYY-MM-DD 

-- The below transactions correspond to manual burns which are accompanied by an equal mint thus they are excluded from our dataset
    AND tx_id != '6L9XW5Yyq6xxpMsYZvzFbHaC5DsscPVdvu9sFYc1v92bJ75fBRHwmbpRoXEHgC9XLrHbgBVmJB559cRuTDccTb6' --1 | mint: HhdirVKCZo98L8ZBNCtHA1KHTPFpFoA6NG51fjfxaYZpTqa8KiJArbhakiEkNg5r5m7BKBQJpKfDyfW9bVzDNxp
    AND tx_id != '4dMZNev5erxjq1ediijXuDjF6hUb1oRpKTZXiUqmb3JCVVraAV2K2SzM6cPzA7nsN3V47rdEsP4b2TgoKNme5G6U' --2 | mint: 5Jq5zZnuymDX1s5C4vGw4DrcvpV27jNczYev3ri5NQWcLnD4gtJLAktdmWzkc9mPP95o48gfPgmM19DZ2dHozxXs
    AND tx_id != '4YrkWW7bCZiBFGz6ohWo12wyM6xGKtgFUmHs7Eh8KHkZc1haVqx4amgvxxm8SBg8A3sMG2kzDeF94v8nfJp75fch' --3 | mint: 2pwtNET2VFCB66hq6sDfiaeULpjjLMBg29CVQ6VH4fsDQPub38nzx2HjmVWNPZzKT91nofiRjVbNFkUA5xcUwTCg
    AND tx_id != '9Z82CFaUDWUaJbToZLhxXaTnofstgEBfthiVbRuy4ZdxskJNquehQFqGxoK9U4hgXHMBSv4CNB6qxUc6wjNzAZK' --4 | mint: 5KuvHPtb14KQ3Us1sQyagNXL9Zm9vN8PxLU2yMo1djseJRPpTaTadMG6wHWoR3ahNq7DSAZwy8ZsuK2DPvah4MX
    AND tx_id != '2x7Ag9Us97mMKAiC5F3MW5PVbwDUyekqF5vxGmbELJTGqeGtmhaD1shCw6UBUYBw5u5kES9e2dve6TcKDV7zJrUL' --5 | mint:  tA9zEWhkCcSn1G9AjxiXPJhvkksgKWHmBYwtsSb69c6esQBartNnumFcRYKJ4pvraZynmVi6h4RuNzuVEN3RZ2D
    AND tx_id != '3sZhmL8PnGQLrh2fnbhgMmbAZmLxhoY9TfDzi8GEPP9yjGNtdTWKbBkW3vXFBfY74jJQqPSroEF631xrMGW4aetL' --6 | mint:MrLxcTVASyuGf1M4sHfRSNoh5d46fhqvQ138DWFd53aZejfKpTk1LkSbNhBJxfRa5Yj9MCvRGZhrWzw2nN3rt6K & 3sLsoC2iSBxBjgWTtY2GFi8Kk1nocGHa3qdXmdsdwuU36Bo33E437Krwk79D3rNAmsaT6zM1Th4KTNKzqL7co5tC
    AND tx_id != '4SQ21aCK84fTgjrAnXCHRuphbFmtm4b3r8s9d92o9p4mhYhY8FLjE5J3zNgsFANyMpdFxqVwLrsNGXW6pt1Hndy4' --7 | mint:3Xm9wZ9YuVckvpgMFfA2jEuiY69JESGE5euRBXUJ6wDL9Yc7qdLpfgZK86PFiajwjNb9Lu9CDTt1mR6dNWAWYvsz
{% if is_incremental() %}
    AND tr.block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}
ORDER BY tr.block_timestamp DESC