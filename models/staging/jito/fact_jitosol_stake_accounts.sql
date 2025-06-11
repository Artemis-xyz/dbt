{{
    config( 
        materialized="incremental",
        snowflake_warehouse="JITO",
        unique_key="address"
    )
}}


WITH all_stake_accounts AS (
    SELECT
        -- Extract relevant stake account based on instruction type
        CASE 
            WHEN LEFT(BASE58_TO_HEX(instruction:data), 2) = '01' THEN instruction:accounts[5]::STRING -- addValidatorToPool (validator stake)
            WHEN LEFT(BASE58_TO_HEX(instruction:data), 2) = '03' THEN instruction:accounts[5]::STRING -- decreaseValidatorStake (transient stake)
            WHEN LEFT(BASE58_TO_HEX(instruction:data), 2) = '04' THEN instruction:accounts[5]::STRING -- increaseValidatorStake (transient stake)
            WHEN LEFT(BASE58_TO_HEX(instruction:data), 2) = '09' THEN instruction:accounts[5]::STRING -- depositStake (deposited stake)
            WHEN LEFT(BASE58_TO_HEX(instruction:data), 2) = '15' THEN instruction:accounts[8]::STRING -- redelegate (destination validator stake)
        END AS stake_account,
        tx_id
    FROM
        {{ source('SOLANA_FLIPSIDE', 'fact_events') }}
    WHERE 1=1
        AND contains(instruction:accounts, '6iQKfEyhr3bZMotVkW6beNZz5CPAkiwvgV2CTje9pVSS') -- Jito Stake Pool Authority
        AND program_id = 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'
        AND LEFT(BASE58_TO_HEX(instruction:data), 2) IN ('01', '03', '04', '09', '15') -- Filter only relevant instructions
        {% if is_incremental() %}
            AND block_timestamp >= (select dateadd('day', -1, max(last_updated)) from {{ this }})
        {% endif %}
    UNION ALL

    -- Explicitly add the reserve stake account
    SELECT
        'BgKUXdS29YcHCFrPm5M8oLHiTzZaMDjsebggjoaQ6KFL' AS stake_account, -- Reserve Stake Account
        NULL AS tx_id
)

SELECT 
    DISTINCT stake_account as address
    , NULL as name
    , 'jito' as artemis_application_id
    , 'solana' as chain
    , NULL as is_token
    , NULL as is_fungible
    , 'lst_pool' as type
    , SYSDATE()::TIMESTAMP_NTZ as last_updated
FROM all_stake_accounts