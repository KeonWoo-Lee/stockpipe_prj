{{ config(materialized='table', tags=['s2r_airbnb']) }}

select
    root.listing_id,
    root.listing_name,
    root.listing_url,
    root.country,
    root.market,
    root.room_type,
    root.property_type,
    root.host_is_superhost,
    root.host_response_rate,
    root.nightly_price,
    root.source_review_count,
    coalesce(review.loaded_review_count, 0) as loaded_review_count,
    coalesce(review.nonempty_comment_count, 0) as nonempty_comment_count,
    review.latest_review_at
from {{ ref('stg_s2r_airbnb__root') }} as root
left join {{ ref('int_s2r_airbnb__review_summary') }} as review
    on root.s2r_record_id = review.s2r_record_id
