{{ config(materialized="table") }}

select address, name, friendly_name, app, chain, category, sub_category
from {{ ref("dim_contracts_silver") }}
