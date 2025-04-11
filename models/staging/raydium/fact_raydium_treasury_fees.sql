{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
    )
}}

select date_trunc('day', block_timestamp) as date
    , mint as token_mint_address
    , sum(amount) as amount_raw -- USDC amount 
from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS
where 1=1
    and mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC
    and tx_to = 'GThUX1Atko4tqhN2NaiTazWSeFWMuiUvfFnyJyUghFMJ'
    and tx_from in (
        'FundHfY8oo8J9KYGyfXFFuQCHe7Z1VBNmsj84eMcdYs4' -- CLMM pools
        , 'FUNDduJTA7XcckKHKfAoEnnhuSud2JUCUZv6opWEjrBU' -- CP-Swap pools
        -- there is no treasury fee being sent from AMMv4 pools
    )

{% if is_incremental() %}
    AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
{% else %}
    AND block_timestamp::date >= date('2022-04-22') 
{% endif %}

group by 1,2
