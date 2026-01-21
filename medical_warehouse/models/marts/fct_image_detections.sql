with detections as (
    select * from {{ ref('stg_yolo_detections') }}
),

classified as (
    select
        *,
        -- Heuristic classification scheme
        case
            when detection_class in ('bottle', 'cup', 'vase', 'bowl', 'wine glass') then 'Product Display'
            when detection_class in ('person', 'handbag', 'tie', 'suitcase') then 'Lifestyle'
            when detection_class in ('tv', 'laptop', 'cell phone', 'book', 'clock') then 'Promotional' -- Assuming devices/text-heavy might be promo
            else 'Other'
        end as image_class
    from detections
)

select * from classified