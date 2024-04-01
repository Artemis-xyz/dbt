select distinct namespace as namespace from {{ ref("dim_dune_contracts_post_sigma") }}
