{{ config(materialized="table") }}

select 
    artemis_application_id,
    initcap(replace(artemis_application_id, '_', ' ')) as app_name,
    artemis_id,
    coingecko_id,
    ecosystem_id,
    defillama_protocol_id,
    visibility,
    symbol as app_symbol,
    icon as app_icon,
from dim_all_apps_gold