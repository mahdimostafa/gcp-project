{{
  config(
    alias = "most_upvoted_and_comments",  
    materialized = "table"
  )
}}

with most_upvoted_and_comments as(
    select 
        created as created_date
      , max(score) as number_of_upvotes
      , max(comms_num) as number_of_comments
 
    from landing.dubai_posts

    group by
        created 
    
)

select 

      created_date
    , sum(number_of_upvotes)
    , sum(number_of_comments)

from most_upvoted_and_comments
  group by 1

