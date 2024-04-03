{{ config(materialized="incremental", unique_key=["package_id", "timestamp", "version"]) }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_sui_contracts") }}
    ),
    sui_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_sui_contracts") }}
        where extraction_date = (select max_date from max_extraction)
    )
    select
        value:"packageId"::string as package_id,
        value:"creator"::string as creator_address,
        value:"packageName"::string as package_name,
        lower(replace(value:"projectName"::string, ' ', '_')) as namespace,
        value:"projectName"::string as friendly_name,
        value:"projectImg"::string as project_img,
        date(to_timestamp(value:"timestamp"::number / 1000)) as timestamp,
        value:"transactions" as transactions,
        value:"version"::number as version
    from sui_data, lateral flatten(input => data)
    order by transactions desc
