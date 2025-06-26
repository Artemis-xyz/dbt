{{ config(
    materialized='table',
    snowflake_warehouse='CARDANO'
) }}

/*
    "circulation": "35356538084557689",
    "deposits_drep": "468500000000",
    "deposits_proposal": "100000000000",
    "deposits_stake": "4377476000000",
    "epoch_no": 564,
    "fees": "64359325919",
    "reserves": "7107661935971911",
    "reward": "748928005518913",
    "supply": "37892338064028089",
    "treasury": "1781861638625568"
*/
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cardano_supply_data") }}
    ), 

    date_spine as (
        select date_trunc('day', block_time) as date, epoch_no
        from {{ ref("fact_cardano_tx") }}
        group by 1, 2
    )
select
    date,
    value:epoch_no::integer as epoch_no,
    value:circulation::number / 1e6 as issued_supply_native,
    value:deposits_drep::number / 1e6 as deposits_drep,
    value:deposits_proposal::number / 1e6 as deposits_proposal,
    value:deposits_stake::number / 1e6 as deposits_stake,
    value:reserves::number / 1e6 as reserves,
    value:reward::number / 1e6 as reward,
    value:supply::number / 1e6 as total_supply_native,
    value:treasury::number / 1e6 as treasury_native, 
    45000000000 as max_supply_native
from
    {{ source("PROD_LANDING", "raw_cardano_supply_data") }},
    lateral flatten(input => parse_json(source_json))
join date_spine on date_spine.epoch_no = value:epoch_no::integer
where extraction_date = (select max_date from max_extraction)
qualify row_number() over (partition by date order by epoch_no desc) = 1