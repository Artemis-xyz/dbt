{{ config(materialized="table", snowflake_warehouse="ANALYTICS_XL") }}


select 
    date
    , t1.chain
    , sum(
        case 
            when t2.type is null then stablecoin_supply
            else 0
        end
    ) as native_supply
    , sum(
        case 
            when t2.type = 'bridged' then stablecoin_supply
            else 0
        end
    ) as bridged_supply
from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }} t1
left join {{ ref("stablecoin_contract_bridged_metadata") }} t2
    on lower(t1.contract_address) = lower(t2.contract_address)
    and t1.chain = t2.chain
group by 1,2
