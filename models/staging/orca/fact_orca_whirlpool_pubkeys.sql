{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
    )
}}

SELECT
    case when f.value:name ilike 'tokenVault%'
        then f.value:pubkey::string
    end as token_vault_pubkey
FROM
    solana_flipside.core.ez_events_decoded,
LATERAL FLATTEN(input => decoded_accounts) AS f

where
    program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
    and event_type ilike '%initializePool%'
    and f.value:name in (
        'tokenVaultA', 'tokenVaultB'
    )