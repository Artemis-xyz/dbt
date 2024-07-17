-- depends_on: {{ source("PROD_LANDING", "raw_akash_active_providers") }}

{{ flatten_cloudmos_json("raw_akash_active_providers", "active_providers") }}
