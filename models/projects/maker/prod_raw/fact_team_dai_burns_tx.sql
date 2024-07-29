{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_team_dai_burns_tx"
    )
}}

SELECT 
    tx_hash,
    usr,
    is_keeper
FROM (
    SELECT 
        d_c_b.tx_hash,
        d_c_b.usr,
        dao_wallet.wallet_label LIKE '% Keepers' as is_keeper
    FROM {{ ref('fact_dai_burn') }} as d_c_b
    JOIN {{ ref('dim_dao_wallet') }} dao_wallet ON d_c_b.usr = dao_wallet.wallet_address

    UNION ALL

    SELECT 
        tx_hash,
        usr,
        FALSE as is_keeper
    FROM {{ ref('fact_dai_burn') }}
    WHERE usr = '0x0048fc4357db3c0f45adea433a07a20769ddb0cf'
)
GROUP BY tx_hash, usr, is_keeper