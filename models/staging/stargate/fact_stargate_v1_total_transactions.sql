{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE_MD",
        alias="fact_stargate_v1_total_transactions.sql"
    )
}}


with
    all_transactions as (
     select * from {{ref("fact_stargate_v1_linea_transactions")}}
     -- union all
     -- select * from {{ref("fact_stargate_v1_ethereum_transactions")}}
    )

select
    *
from all_transactions
