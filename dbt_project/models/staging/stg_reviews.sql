-- stg_reviews: clean order reviews
with source as (
    select * from {{ source('raw', 'order_reviews') }}
),

cleaned as (
    select
        review_id,
        order_id,
        review_score,
        case
            when review_score >= 4 then 'positive'
            when review_score = 3  then 'neutral'
            else 'negative'
        end                               as sentiment,
        review_comment_title              as title,
        review_comment_message            as comment,
        review_creation_date::timestamp   as created_at,
        review_answer_ts::timestamp       as answered_at
    from source
    where review_id is not null
)

select * from cleaned
