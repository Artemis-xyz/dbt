{{config(materialized="incremental")}}

{{ flipside_cleaned_events("base") }}