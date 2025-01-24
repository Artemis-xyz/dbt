{{
    config(
        materialized="table",
        snowflake_warehouse="STABLECOIN_V2_LG"
    )
}}

with stablecoin_transfers as (
    {{ l30d_stablecoin_transfers('base') }}
    union all 
    {{ l30d_stablecoin_transfers('arbitrum') }}
    union all
    {{ l30d_stablecoin_transfers('optimism') }}
    union all
    {{ l30d_stablecoin_transfers('avalanche') }}
    union all
    {{ l30d_stablecoin_transfers('polygon') }}
    union all
    {{ l30d_stablecoin_transfers('ethereum') }}
    union all
    {{ l30d_stablecoin_transfers('solana') }}
    union all
    {{ l30d_stablecoin_transfers('tron') }}
    union all
    {{ l30d_stablecoin_transfers('bsc') }}
    union all
    {{ l30d_stablecoin_transfers('ton') }}
    union all
    {{ l30d_stablecoin_transfers('celo') }}
    union all
    {{ l30d_stablecoin_transfers('mantle') }}
    union all
    {{ l30d_stablecoin_transfers('sui') }}
)
select
    stablecoin_transfers.date
    , stablecoin_transfers.chain
    , stablecoin_transfers.symbol
    , stablecoin_transfers.contract_address
    , stablecoin_transfers.from_address
    , t1.friendly_name as from_application
    , t1.artemis_application_id as from_app
    , t1.artemis_category_id as from_category
    , a1.icon as from_icon
    , stablecoin_transfers.to_address
    , t2.friendly_name as to_application
    , t2.artemis_application_id as to_app
    , t2.artemis_category_id as to_category
    , a2.icon as to_icon
    , inflow * {{ waterfall_stablecoin_prices('stablecoin_transfers', 'd') }} as inflow
    , transfer_volume * {{ waterfall_stablecoin_prices('stablecoin_transfers', 'd') }} as transfer_volume
from stablecoin_transfers
left join {{ ref('dim_all_addresses_labeled_gold')}} t1 
    on lower(stablecoin_transfers.from_address) = lower(t1.address) 
    and lower(stablecoin_transfers.chain) = lower(t1.chain)
left join {{ ref('dim_apps_gold')}} a1 
    on lower(t1.app) = lower(a1.namespace)
left join {{ ref('dim_all_addresses_labeled_gold')}} t2 
    on lower(stablecoin_transfers.to_address) = lower(t2.address) 
    and lower(stablecoin_transfers.chain) = lower(t2.chain)
left join {{ ref('dim_apps_gold')}} a2
    on lower(t2.app) = lower(a2.namespace)
left join {{ ref( "fact_coingecko_token_date_adjusted_gold") }} d
    on lower(stablecoin_transfers.coingecko_id) = lower(d.coingecko_id)
    and stablecoin_transfers.date = d.date::date