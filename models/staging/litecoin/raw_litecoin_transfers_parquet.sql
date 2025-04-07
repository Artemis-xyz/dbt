{{ config(materialized='table') }}

create or replace table LANDING_DATABASE.PROD_LANDING.RAW_LITECOIN_TRANSFERS_PARQUET (
    PARQUET_RAW VARIANT
); 