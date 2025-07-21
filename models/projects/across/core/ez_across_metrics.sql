{{
    config(
        materialized="incremental",
        snowflake_warehouse="ACROSS",
        database="across",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        full_refresh=false,
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with
    bridge_volume as (
        select date, bridge_volume
        from {{ ref("fact_across_bridge_volume") }}
        where chain is null
        {% if is_incremental() %}
            {% if backfill_date %}
                and date >= '{{ backfill_date }}'
            {% else %}
                and date > (select max(this.date) from {{ this }} as this)
            {% endif %}
        {% endif %}
    ),
    bridge_daa as (
        select date, bridge_daa
        from {{ ref("fact_across_bridge_daa") }}
        {% if is_incremental() %}
            {% if backfill_date %}
                where date >= '{{ backfill_date }}'
            {% else %}
                where date > (select max(this.date) from {{ this }} as this)
            {% endif %}
        {% endif %}
    )
    , price_data as ({{ get_coingecko_metrics("across") }})
select
    bridge_volume.date as date
    , 'across' as app
    , 'Bridge' as category
    , bridge_daa.bridge_daa
    -- Standardized Metrics
    , bridge_volume.bridge_volume as bridge_volume
    , bridge_daa.bridge_daa as bridge_dau
    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from bridge_volume
left join bridge_daa on bridge_volume.date = bridge_daa.date
left join price_data on bridge_volume.date = price_data.date
where bridge_volume.date < to_date(sysdate())
