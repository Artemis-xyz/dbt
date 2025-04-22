-- IMPORTANT: if you are adding a new protocol here, please make sure the value/s in the artemis_application_id column exists in dim_all_apps_gold!!!
-- If it doesn't exist, please create a new app on the Onchain Explorer for this protocol!
-- If confused, please reach out to Horace.
{{
    config( 
        materialized="table"
    )
}}


{{ dbt_utils.union_relations(
    relations=[
        ref("fact_jitosol_stake_accounts"),
        ref("fact_orca_treasury_accounts"),
    ]
)}}