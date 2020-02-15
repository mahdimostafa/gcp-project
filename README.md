# gcp-project
starter project using gcp &amp; dbt


# LOAD SCRIPTS

First build & load (One time only to grab one months dataset):
dubai_api.py

Scheduled daily load:
dubai_stream.py

# DBT
Two models created and to be executed daily along with dubai_steam.py via cloud composer
```
models:

Test:
dbt compile --models count_of_submissions
dbt compile --models most_upvoted_and_comments

dev

dbt run --profile dev_gcp --models count_of_submissions
dbt run --profile dev_gcp --models most_upvoted_and_comments

prod
dbt run --profile dev_gcp --models count_of_submissions
dbt run --profile dev_gcp --models most_upvoted_and_comments


Rows should return from running in BigQuery:
select * from data_mart.count_of_submissions limit 10;
select * from data_mart.most_upvoted_and_comments limit 10;

```

# Release management flow

![alt text](https://i.stack.imgur.com/hlYjj.png)

# Cloud Composer (airflow)(WIP)

![alt text](https://github.com/mahdimostafa/gcp-project/blob/master/dag_graph_view.png?raw=true)

# Data Architecture

![alt text](https://github.com/mahdimostafa/gcp-project/blob/master/data.png?raw=true)
