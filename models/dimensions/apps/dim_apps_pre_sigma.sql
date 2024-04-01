{{ config(materialized="table") }}

select
    namespace,
    friendly_name,
    sub_category,
    category,
    artemis_id,
    coingecko_id,
    ecosystem_id,
    defillama_protocol_id,
    parent_app,
    visibility,
    symbol,
    icon
from {{ ref("dim_apps_gold") }}
