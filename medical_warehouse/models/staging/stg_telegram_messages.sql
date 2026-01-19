
with source as (
    select * from {{ source('raw', 'telegram_messages') }}
),

cleaned as (
    select
        id as raw_id,
        message_id,
        channel_name,
        -- Cast message_date to timestamp
        message_date::timestamp as message_date,
        -- Handle null message_text
        coalesce(message_text, '') as message_text,
        -- Calculate message length
        length(coalesce(message_text, '')) as message_length,
        has_media,
        image_path,
        coalesce(views, 0) as views,
        coalesce(forwards, 0) as forwards
    from source
    where message_date is not null 
      and channel_name is not null
)

select * from cleaned