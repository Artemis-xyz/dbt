-- IMPORTANT: if you are adding a new protocol here, please make sure the value/s in the artemis_application_id column exists in dim_all_apps_gold!!!
-- If it doesn't exist, please create a new app on the Onchain Explorer for this protocol!
-- If confused, please reach out to Horace.
{{
    config( 
        materialized="table"
    )
}}

SELECT 
    address,
    name,
    artemis_application_id,
    chain,
    is_token,
    is_fungible,
    type,
    last_updated
FROM {{ ref("fact_jitosol_stake_accounts") }}

UNION ALL 

SELECT 
    address,
    name,
    artemis_application_id,
    chain,
    is_token,
    is_fungible,
    type,
    last_updated
FROM {{ ref("fact_maple_treasury_accounts") }}