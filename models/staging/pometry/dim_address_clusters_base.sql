{{ config(materialized = 'table') }}


select parquet_raw:address::string as address, parquet_raw:reduce_label::string as cluster_id
from {{ source("PROD_LANDING", "raw_base_pometry_clusters_parquet") }}
where inserted_at = (select max(inserted_at) from {{ source("PROD_LANDING", "raw_base_pometry_clusters_parquet") }})  