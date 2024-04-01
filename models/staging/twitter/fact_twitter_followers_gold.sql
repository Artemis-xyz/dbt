{{ config(materialized="table") }}
select date, twitter_handle, follower_count
from {{ ref("fact_twitter_followers") }}
