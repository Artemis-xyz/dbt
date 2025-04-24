{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="JUPITER",
    )
}}

SELECT
    block_timestamp::date as date,
    instruction_name,
    case when
        platform_fee_bps in (5, 10) and block_timestamp > '2025-01-25' -- There exist fees with these bps prior to Ultra, but they are negligible.
            THEN 'Ultra' 
        ELSE 'Referral'
    END as swap_type,
    count(*) as swap_count,
    count(distinct user_address) as dau,
    sum(fee_amount_usd) as swap_fees, -- includes ultra and referral fees
    sum(coalesce(token_in_amount_usd, token_out_amount_usd)) as volume
FROM
    {{ ref('fact_jupiter_swap_txs') }}
WHERE 1=1
    AND fee_amount_usd is not null
    AND ABS(COALESCE(log(10, nullif(token_in_amount_usd,0) / nullif(token_out_amount_usd,0)),0)) < 2 -- This filters a few key bad data points
    AND 
        (coalesce(token_in_amount_usd, token_out_amount_usd) < 1e7  -- no swaps larger than 10m
            OR ( -- Unless one of these tokens involved in the trade
                (token_in_symbol in ('USDC', 'USDT', 'SOL', 'JITOSOL', 'PYUSD', 'ETH')
                OR token_out_symbol in ('USDC', 'USDT', 'SOL', 'JITOSOL', 'PYUSD', 'ETH'))
                AND coalesce(token_in_amount_usd, token_out_amount_usd) < 1e8 -- Definitely no swaps over 100m
                )
        )
    AND succeeded
    AND coalesce(token_in_address, token_out_address) not in (
        '2TLDx5M7Z9pfUPbHAboYeTEq6ShzaGhnCwWkfvVyPFyD', 
        '6FupkbAC2UvnqFYZp69yJ2S3BYo1Va8V9jTho9wJpump',
        'v62Jv9pwMTREWV9f6TetZfMafV254vo99p7HSF25BPr',
        '4h41QKUkQPd2pCAFXNNgZUyGUxQ6E7fMexaZZHziCvhh'
    ) -- Filter these bad tokens
    AND fee_amount_usd < 1e5
GROUP BY 1, 2, 3