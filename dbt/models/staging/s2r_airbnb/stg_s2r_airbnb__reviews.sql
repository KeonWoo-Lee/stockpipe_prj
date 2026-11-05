{{ config(materialized='view', tags=['s2r_airbnb']) }}

select
    __etl_record_id as review_record_id,
    __etl_parent_record_id as parent_record_id,
    __etl_array_index as review_array_index,
    "reviews._id" as review_id,
    "reviews.listing_id" as listing_id,
    "reviews.reviewer_id" as reviewer_id,
    "reviews.reviewer_name" as reviewer_name,
    "reviews.comments" as review_comment,
    nullif(btrim("reviews.date.$date"), '')::timestamptz as reviewed_at
from {{ source('s2r_airbnb', 's2r_reviews') }}
