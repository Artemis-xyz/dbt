{{ config(materialized = 'table') }}


select parquet_raw:node::string as address, parquet_raw:reduced_node::string as cluster_id
from {{ source("PROD_LANDING", "raw_base_pometry_clusters_parquet") }}
where inserted_at = (select max(inserted_at) from {{ source("PROD_LANDING", "raw_base_pometry_clusters_parquet") }})  