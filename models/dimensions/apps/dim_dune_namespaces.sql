select 
    namespace,
    max(last_updated) as last_updated
from {{ ref("dim_dune_contracts_post_sigma") }}
group by namespace