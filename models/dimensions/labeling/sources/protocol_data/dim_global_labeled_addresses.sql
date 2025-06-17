-- IMPORTANT: if you are adding a new protocol here, please make sure the value/s in the artemis_application_id column exists in dim_all_apps_gold!!!
-- If it doesn't exist, please create a new app on the Onchain Explorer for this protocol!
-- If confused, please reach out to Horace.
{{
    config( 
        materialized="table"
    )
}}

-- Unioned tables require columns: address, name, artemis_application_id, chain, is_token, is_fungible, type, last_updated
with unioned_labels as (
    {{ dbt_utils.union_relations(
        relations=[
                ref("fact_jitosol_stake_accounts"),
                ref("fact_orca_treasury_accounts"),
                ref("fact_maple_treasury_accounts"),
                ref("fact_jupitersol_stake_accounts"),
                ref("fact_meteora_lbpair_pools"),
                ref("fact_meteora_lbpair_vaults"),
                ref("fact_maker_tvl_addresses")
            ]
        )
    }}
)


select * EXCLUDE(_dbt_source_relation)
from unioned_labels
qualify row_number() over (partition by address, chain order by last_updated desc) = 1
