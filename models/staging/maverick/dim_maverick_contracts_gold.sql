{{ config(materialized="table", snowflake_warehouse="DEX_SM") }}

select chain, app, address, name, version, category
from {{ ref("dim_maverick_contracts") }}
