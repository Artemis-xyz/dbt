{{
    config(
        materialized="view",
        database="common",
        schema="core",
        snowflake_warehouse="COMMON",
    )
}}

select 
    *
from {{ ref("fact_protocol_datahub_gold") }}
where date < to_date(sysdate())
order by date desc, artemis_id asc