{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_coingecko_token_metadata") }}
    ),
    token_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_coingecko_token_metadata") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_categories as (
        select
            date,
            data:id::string as coingecko_token_id,
            array_agg(value::string) as token_categories
        from token_data, lateral flatten(input => data:categories)  -- Flatten the categories array
        group by date, coingecko_token_id
    ),
    flattened_homepage as (
        select
            date,
            data:id::string as coingecko_token_id,
            array_agg(value::string) as token_homepage_link
        from token_data, lateral flatten(input => data:links:homepage)  -- Flatten the homepage array
        group by date, coingecko_token_id
    ),
    flattened_chat_url as (
        select
            date,
            data:id::string as coingecko_token_id,
            array_agg(value::string) as token_chat_url
        from token_data, lateral flatten(input => data:links:chat_url)  -- Flatten the chat_url array
        group by date, coingecko_token_id
    ),
    flattened_data as (
        select
            date,
            data:id::string as coingecko_token_id,
            data:symbol::string as token_symbol,
            data:name::string as token_name,
            data:links:subreddit_url::string as token_subreddit_url,
            data:links:twitter_screen_name::string as token_twitter_screen_name,
            data:description:en::string as token_description,
            data:image:thumb::string as token_image_thumb,
            data:image:small::string as token_image_small,
            data:market_data:market_cap_rank::int as token_market_cap_rank
        from token_data
    )

select fd.*, fc.token_categories, fh.token_homepage_link, fcu.token_chat_url
from flattened_data fd
left join flattened_categories fc using (date, coingecko_token_id)
left join flattened_homepage fh using (date, coingecko_token_id)
left join flattened_chat_url fcu using (date, coingecko_token_id)
where
    coingecko_token_id is not null
order by token_market_cap_rank asc
