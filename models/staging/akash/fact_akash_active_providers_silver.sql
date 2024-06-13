-- depends_on: {{ source("PROD_LANDING", "raw_akash_active_providers") }}
{{ config(snowflake_warehouse="AKASH") }}

{{ flatten_cloudmos_json("raw_akash_active_providers", "active_providers") }}
