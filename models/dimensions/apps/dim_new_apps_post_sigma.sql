select
    namespace,
    friendly_name,
    sub_category,
    category,
    coingecko_id,
    parent_namespace,
    artemis_id,
    ecosystem_id,
    cast(defillama_protocol_id as integer) as defillama_protocol_id,
    symbol
from {{ source("SIGMA", "sigma_add_new_app") }}
where namespace is not null
