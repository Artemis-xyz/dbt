{{ config(materialized='table') }}

create or replace table LANDING_DATABASE.PROD_LANDING.RAW_LITECOIN_INPUTS_PARQUET (
    PARQUET_RAW VARIANT
); 