{{config(materialized='table')}}
with
categories as (
    select 
        coalesce(
            block_buster.address
            , bob_the_builder.address
            , botimus_prime.address
            , terminally_based.address
            , wolf_of_wallstreet.address
            , dora_the_explorer.address
            , old_mcdonald.address
            , boomer.address
        ) as address
        , coalesce(
            block_buster.category
            , bob_the_builder.category
            , botimus_prime.category
            , terminally_based.category
            , wolf_of_wallstreet.category
            , dora_the_explorer.category
            , old_mcdonald.category
            , boomer.category
        ) as category
        , coalesce(
            block_buster.reason::string
            , bob_the_builder.reason::string
            , botimus_prime.reason::string
            , terminally_based.reason::string
            , ARRAY_TO_STRING(wolf_of_wallstreet.reason, ', ')
            , dora_the_explorer.reason::string
            , ARRAY_TO_STRING(old_mcdonald.reason, ', ')
            , ARRAY_TO_STRING(boomer.reason, ', ')
        ) as reason
    from {{ref('dim_blockbuster')}} block_buster
    full outer join {{ref('dim_bob_the_builder')}} bob_the_builder using (address)
    full outer join {{ref('dim_botimus_prime')}} botimus_prime using (address)
    full outer join {{ref('dim_terminally_based')}} terminally_based using (address)
    full outer join {{ref('dim_wolf_of_wallstreet')}} wolf_of_wallstreet using (address)
    full outer join {{ref('dim_dora_the_explorer')}} dora_the_explorer using (address)
    full outer join {{ref('dim_old_mcdonald')}} old_mcdonald using (address)
    full outer join {{ref('dim_boomer')}} boomer using (address)
)

select 
    address
    , coalesce(category, 'NORMIE') as category
    , reason
    , total_txns
    , total_txns_percent_rank as total_txns_percent_rank
    , total_gas_paid
    , total_gas_paid_percent_rank as total_gas_paid_percent_rank
    , days_onchain
    , days_onchain_percent_rank as days_onchain_percent_rank
    , apps_used
    , apps_used_percent_rank as apps_used_percent_rank
from {{ref('agg_artemis_wrapped_metrics')}}
left join categories using (address)