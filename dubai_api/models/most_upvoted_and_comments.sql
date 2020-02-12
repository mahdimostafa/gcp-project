{{
  config(
    alias = "most_upvoted_and_comments",  
    materialized = "table"
  )
}}

with most_upvoted_and_comments as(
    select 
        created as created_date
      , sum(score) as number_of_upvotes
      , sum(comms_num) as number_of_comments
 
    from landing.dubai_posts

    group by
        created 
    
)

select 

      created_date
    , number_of_upvotes
    , number_of_comments

from most_upvoted_and_comments

