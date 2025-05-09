{{ config(materialized="table", snowflake_warehouse="STABLECOIN_DAILY") }}
select
    date,
    from_address,
    coalesce(from_label.artemis_application_id, from_address) as from_label,
    to_address,
    coalesce(to_label.artemis_application_id, to_address) as to_label,
    symbol,
    count(*) AS total_transfers,
    sum(amount) AS total_amount
from {{ ref('fact_tron_stablecoin_transfers') }}
left join {{ ref('dim_all_addresses_labeled_gold')}} from_label on lower(from_address) = lower(from_label.address)
left join {{ ref('dim_all_addresses_labeled_gold')}} to_label on lower(to_address) = lower(to_label.address)
group by date, from_address, from_label, to_address, to_label, symbol
order by date asc