{{
  config(
    alias = "count_of_submissions",  
    materialized = "table"
  )
}}

with count_of_submissions as(
    select 
          created as created_date
        , count(title) as number_of_posts
 
    from  landing.dubai_posts

    where
          created >= date_add(current_date(), interval -30 day)

    group by
          created 
    
)

select 

      created_date
    , number_of_posts


from count_of_submissions

