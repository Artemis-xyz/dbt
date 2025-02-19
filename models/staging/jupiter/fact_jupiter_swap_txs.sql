{{
    config(
        materialized='incremental',
        unique_key=['tx_id', 'index', 'inner_index'],
        snowflake_warehouse='MEDIUM',
    )
}}

WITH deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY TX_ID, INDEX, INNER_INDEX ORDER BY BLOCK_TIMESTAMP DESC) AS row_num
    FROM (
        SELECT 
            e.BLOCK_TIMESTAMP,
            e.TX_ID,
            e.INDEX,
            e.INNER_INDEX, -- Usually null but there are some transactions with inner index
            e.PROGRAM_ID,
            e.EVENT_TYPE AS instruction_name,

            -- User Info
            e.DECODED_INSTRUCTION:accounts[1].pubkey::string AS user_address, -- user_transfer_authority

            
            -- IN token
            CASE 
                WHEN e.EVENT_TYPE IN ('exact_out_route', 'shared_accounts_exact_out_route', 'exactOutRoute', 'sharedAccountsExactOutRoute') 
                THEN e.DECODED_ARGS:quotedInAmount::FLOAT
                ELSE e.DECODED_ARGS:inAmount::FLOAT
            END AS token_in_amount_raw,
            p_in.decimals as token_in_decimals,
            (token_in_amount_raw / POWER(10, p_in.DECIMALS)) * p_in.PRICE AS token_in_amount_usd,
            token_in_amount_raw / POWER(10, p_in.DECIMALS) AS token_in_amount_native,
            p_in.symbol as token_in_symbol,
            COALESCE(
                -- If `source_mint` is explicitly provided in the instruction, use it
                CASE 
                    WHEN e.EVENT_TYPE like 'shared_accounts_%' or e.EVENT_TYPE like 'sharedAccounts%'
                        THEN e.DECODED_INSTRUCTION:accounts[7].pubkey -- Correct index for these instructions
                    WHEN e.EVENT_TYPE = 'exact_out_route' or e.EVENT_TYPE = 'exactOutRoute'
                        THEN e.DECODED_INSTRUCTION:accounts[5].pubkey -- Correct index for these instructions
                END,
                -- If missing, get `source_mint` from token account mapping
                tam.mint
            ) AS token_in_address, 

            -- OUT token
            CASE 
                WHEN e.EVENT_TYPE IN (
                        'route', 
                        'shared_accounts_route', 'sharedAccountsRoute',
                        'route_with_token_ledger', 'routeWithTokenLedger',
                        'shared_accounts_route_with_token_ledger', 'sharedAccountsRouteWithTokenLedger'
                    ) 
                THEN e.DECODED_ARGS:quotedOutAmount::FLOAT
                ELSE e.DECODED_ARGS:outAmount::FLOAT 
            END AS token_out_amount_raw,
            p_out.decimals as token_out_decimals,
            (token_out_amount_raw / POWER(10, p_out.DECIMALS)) * p_out.PRICE AS token_out_amount_usd,
            token_out_amount_raw / POWER(10, p_out.DECIMALS) AS token_out_amount_native,
            p_out.symbol as token_out_symbol,
            CASE 
                WHEN e.EVENT_TYPE like 'shared_accounts_%' or e.EVENT_TYPE like 'sharedAccounts%'
                    THEN e.DECODED_INSTRUCTION:accounts[8].pubkey::STRING -- Correct index for these instructions
                WHEN e.EVENT_TYPE = 'exact_out_route' or e.EVENT_TYPE = 'exactOutRoute'
                    THEN e.DECODED_INSTRUCTION:accounts[6].pubkey -- Correct index for these instructions
                ELSE e.DECODED_INSTRUCTION:accounts[5].pubkey::STRING -- Correct for `route`, `exact_out_route`, `route_with_token_ledger`
            END AS token_out_address,

            -- Fee token information
            e.DECODED_ARGS:platformFeeBps::NUMBER AS platform_fee_bps,

            -- If token OUT information is missing, use token IN information to calculate fees
            -- Fee is paid in the OUT token
            COALESCE(token_out_amount_usd, token_in_amount_usd) * platform_fee_bps / 10000 AS fee_amount_usd,
            COALESCE(token_out_amount_native, token_in_amount_native) * platform_fee_bps / 10000 AS fee_amount_native,        
            COALESCE(p_out.symbol, p_in.symbol) as fee_token_symbol,
            COALESCE(token_out_address, token_in_address) as fee_token_address,

            -- Platform fee account used to determine Ultra Fees
            
            (CASE 
                WHEN e.EVENT_TYPE = 'exact_out_route' or e.EVENT_TYPE = 'exactOutRoute'
                    THEN e.DECODED_INSTRUCTION:accounts[4].pubkey
                WHEN e.EVENT_TYPE like 'shared_accounts_%' or e.EVENT_TYPE like 'sharedAccounts%'
                    THEN e.DECODED_INSTRUCTION:accounts[9].pubkey
                ELSE e.DECODED_INSTRUCTION:accounts[6].pubkey -- Correct for `route`, `route_with_token_ledger`
            END)::string AS platform_fee_account,
            COALESCE(fao.owner, platform_fee_account) as platform_fee_account_owner,  -- Need to coalesce because Jup v6 program does not have an owner

            e.SUCCEEDED

        FROM 
            SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED e

        -- Join to get `source_mint` using `user_source_token_account`
        LEFT JOIN pc_dbt_db.prod.fact_solana_token_account_to_mint tam
            ON tam.account_address = e.DECODED_INSTRUCTION:accounts[2].pubkey  -- user_source_token_account

        -- Join to get the fee account owner
        LEFT JOIN pc_dbt_db.prod.fact_solana_token_account_to_mint fao
            ON fao.account_address =
                (CASE 
                    WHEN e.EVENT_TYPE = 'exact_out_route' or e.EVENT_TYPE = 'exactOutRoute'
                        THEN e.DECODED_INSTRUCTION:accounts[4].pubkey
                    WHEN e.EVENT_TYPE like 'shared_accounts_%' or e.EVENT_TYPE like 'sharedAccounts%'
                        THEN e.DECODED_INSTRUCTION:accounts[9].pubkey
                    ELSE e.DECODED_INSTRUCTION:accounts[6].pubkey -- Correct for `route`, `route_with_token_ledger
                END)::string  -- user_source_token_account


        LEFT JOIN solana_flipside.price.ez_prices_hourly p_in
            ON p_in.TOKEN_ADDRESS = COALESCE(
                -- Use `source_mint` from instruction if it exists
                CASE 
                    WHEN e.EVENT_TYPE like 'shared_accounts_%' or e.EVENT_TYPE like 'sharedAccounts%'
                        THEN e.DECODED_INSTRUCTION:accounts[7].pubkey -- Correct index for these instructions
                    WHEN e.EVENT_TYPE = 'exact_out_route' or e.EVENT_TYPE = 'exactOutRoute'
                        THEN e.DECODED_INSTRUCTION:accounts[5].pubkey -- Correct index for these instructions
                END,
                -- If missing, get `source_mint` from token account mapping
                tam.mint
            )  
            AND p_in.HOUR = DATE_TRUNC('hour', e.BLOCK_TIMESTAMP)


        LEFT JOIN solana_flipside.price.ez_prices_hourly p_out
            ON p_out.TOKEN_ADDRESS =
                CASE 
                    WHEN e.EVENT_TYPE like 'shared_accounts_%' or e.EVENT_TYPE like 'sharedAccounts%'
                        THEN e.DECODED_INSTRUCTION:accounts[8].pubkey::STRING -- Correct index for these instructions
                    WHEN e.EVENT_TYPE = 'exact_out_route' or e.EVENT_TYPE = 'exactOutRoute'
                        THEN e.DECODED_INSTRUCTION:accounts[6].pubkey -- Correct index for these instructions
                    ELSE e.DECODED_INSTRUCTION:accounts[5].pubkey::STRING -- Correct for `route`, `exact_out_route`, `route_with_token_ledger`
                END
            AND p_out.HOUR = DATE_TRUNC('hour', e.BLOCK_TIMESTAMP)


        WHERE e.PROGRAM_ID = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
            AND e.EVENT_TYPE IN (
                    'route', 
                    -- Account for camel and snake case
                    'route_with_token_ledger', 'routeWithTokenLedger', 
                    'shared_accounts_route', 'sharedAccountsRoute',
                    'shared_accounts_exact_out_route', 'sharedAccountsExactOutRoute',
                    'exact_out_route', 'exactOutRoute',
                    'shared_accounts_route_with_token_ledger', 'sharedAccountsRouteWithTokenLedger'
            )
            {% if is_incremental() %}
                AND e.BLOCK_TIMESTAMP > (SELECT DATEADD(day, -3, MAX(BLOCK_TIMESTAMP)) FROM {{ this }})
            {% endif %}
    )
)

select * from deduplicated
WHERE row_num = 1