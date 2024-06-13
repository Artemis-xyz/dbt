-- depends_on: {{ source("PROD_LANDING", "raw_akash_new_leases") }}
{{ config(snowflake_warehouse="AKASH") }}

{{ flatten_cloudmos_json("raw_akash_new_leases", "new_leases") }}
