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