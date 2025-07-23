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
    union all
    {{ l30d_stablecoin_transfers('stellar') }}
    union all
    {{ l30d_stablecoin_transfers('sei') }}
    union all
    {{ l30d_stablecoin_transfers('hyperevm') }}
    union all
    {{ l30d_stablecoin_transfers('katana') }}
    union all
    {{ l30d_stablecoin_transfers('aptos') }}
    union all
    {{ l30d_stablecoin_transfers('ripple') }}
    union all
    {{ l30d_stablecoin_transfers('kaia') }}
    union all
    {{ l30d_stablecoin_transfers('sonic') }}
)
select
    stablecoin_transfers.date
    , stablecoin_transfers.chain
    , stablecoin_transfers.symbol
    , stablecoin_transfers.contract_address
    , stablecoin_transfers.from_address
    , t1.friendly_name as from_application
    , t1.artemis_application_id as from_app
    , case 
        when t1.artemis_sub_category_id = 'market_maker' then t1.artemis_sub_category_id
        when t1.artemis_sub_category_id = 'cex' then t1.artemis_sub_category_id
        else t1.artemis_category_id 
    end as from_category
    , a1.icon as from_icon
    , stablecoin_transfers.to_address
    , t2.friendly_name as to_application
    , t2.artemis_application_id as to_app
    , case 
        when t2.artemis_sub_category_id = 'market_maker' then t2.artemis_sub_category_id
        when t2.artemis_sub_category_id = 'cex' then t2.artemis_sub_category_id
        else t2.artemis_category_id 
    end as to_category
    , a2.icon as to_icon
    , inflow * {{ waterfall_stablecoin_prices('stablecoin_transfers', 'd') }} as inflow
    , transfer_volume * {{ waterfall_stablecoin_prices('stablecoin_transfers', 'd') }} as transfer_volume
from stablecoin_transfers
left join {{ ref('dim_all_addresses_labeled_gold')}} t1 
    on lower(stablecoin_transfers.from_address) = lower(t1.address) 
    and lower(stablecoin_transfers.chain) = lower(t1.chain)
left join {{ ref('dim_all_apps_gold')}} a1 
    on lower(t1.artemis_application_id) = lower(a1.artemis_application_id)
left join {{ ref('dim_all_addresses_labeled_gold')}} t2 
    on lower(stablecoin_transfers.to_address) = lower(t2.address) 
    and lower(stablecoin_transfers.chain) = lower(t2.chain)
left join {{ ref('dim_all_apps_gold')}} a2
    on lower(t2.artemis_application_id) = lower(a2.artemis_application_id)
left join {{ ref( "fact_coingecko_token_date_adjusted_gold") }} d
    on lower(stablecoin_transfers.coingecko_id) = lower(d.coingecko_id)
    and stablecoin_transfers.date = d.date::date