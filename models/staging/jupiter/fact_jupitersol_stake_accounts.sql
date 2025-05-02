{{
    config( 
        materialized="incremental",
        snowflake_warehouse="JUPITER",
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
        END AS stake_account
    FROM
        {{ source('SOLANA_FLIPSIDE', 'fact_events') }}
    WHERE 1=1
        AND contains(instruction:accounts, 'EMjuABxELpYWYEwjkKmQKBNCwdaFAy4QYAs6W9bDQDNw') -- Jupiter Stake Pool Authority
        AND program_id = 'SPMBzsVUuoHA4Jm6KunbsotaahvVikZs1JyTW6iJvbn'
        AND LEFT(BASE58_TO_HEX(instruction:data), 2) IN ('01', '03', '04', '09', '15') -- Filter only relevant instructions
        AND stake_account != 'SysvarStakeHistory1111111111111111111111111' -- Filter out only the relevant stake account
        {% if is_incremental() %}
            AND block_timestamp >= (select dateadd('day', -1, max(last_updated)) from {{ this }})
        {% endif %}
)

SELECT 
    DISTINCT stake_account as address
    , NULL as name
    , 'jupiter' as artemis_application_id
    , 'solana' as chain
    , NULL as is_token
    , NULL as is_fungible
    , 'lst_pool' as type
    , SYSDATE()::TIMESTAMP_NTZ as last_updated
FROM all_stake_accounts