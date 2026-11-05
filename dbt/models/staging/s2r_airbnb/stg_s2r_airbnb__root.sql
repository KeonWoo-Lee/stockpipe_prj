{{ config(materialized='view', tags=['s2r_airbnb']) }}

select
    __etl_record_id as s2r_record_id,
    "_id" as listing_id,
    name as listing_name,
    listing_url,
    room_type,
    property_type,
    "address.country" as country,
    "address.market" as market,
    "host.host_is_superhost" as host_is_superhost,
    "host.host_response_rate" as host_response_rate,
    number_of_reviews as source_review_count,
    "reviews_per_month" as reviews_per_month,
    nullif(btrim("price.$numberDecimal"), '')::numeric as nightly_price
from {{ source('s2r_airbnb', 's2r_root') }}

