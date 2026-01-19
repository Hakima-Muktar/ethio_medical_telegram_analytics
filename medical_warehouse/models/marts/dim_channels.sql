
with messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channel_stats as (
    select
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(views) as avg_views
    from messages
    group by 1
),

channels_with_type as (
    select
        channel_name,
        case
            when lower(channel_name) like '%pharma%' then 'Pharmaceutical'
            when lower(channel_name) like '%cosmetics%' then 'Cosmetics'
            when lower(channel_name) like '%medical%' or lower(channel_name) like '%chemed%' then 'Medical'
            else 'General'
        end as channel_type,
        first_post_date,
        last_post_date,
        total_posts,
        avg_views
    from channel_stats
)

select
    row_number() over (order by channel_name) as channel_key,
    *
from channels_with_type