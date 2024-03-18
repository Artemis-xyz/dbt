{{ config(materialized="table") }}
select date, txns, chain
from {{ ref("fact_scroll_txns") }}
