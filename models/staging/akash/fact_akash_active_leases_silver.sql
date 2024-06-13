-- depends_on: {{ source("PROD_LANDING", "raw_akash_active_leases") }}
{{ config(snowflake_warehouse="AKASH") }}

{{ flatten_cloudmos_json("raw_akash_active_leases", "active_leases") }}
