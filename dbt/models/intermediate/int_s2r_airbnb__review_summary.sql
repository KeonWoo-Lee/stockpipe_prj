{{ config(materialized='view', tags=['s2r_airbnb']) }}

select
    parent_record_id as s2r_record_id,
    count(*) as loaded_review_count,
    count(*) filter (
        where nullif(btrim(review_comment), '') is not null
    ) as nonempty_comment_count,
    max(reviewed_at) as latest_review_at
from {{ ref('stg_s2r_airbnb__reviews') }}
where parent_record_id is not null
group by parent_record_id
