import os, sys
import praw
from psaw import PushshiftAPI
import pandas as pd
import datetime as dt
from datetime import timedelta
import emoji
from unidecode import unidecode
from google.cloud import storage
from google.cloud import bigquery
from requests.exceptions import HTTPError

"""
    CREATE A CONNECTION TO REDDIT
"""


def red_connect():
    try:
        reddit = praw.Reddit('CONFIG',
                             user_agent='dubai posts'
                             )
        
        api = PushshiftAPI(reddit)
    except HTTPError as http_err:
        print('HTTP error occurred: {}'.format(http_err))
        print('System abort!')
        sys.exit()
    except Exception as e:
        print('Failed to request data from Reddit API.')
        print('System abort!')
        print(str(e))
        sys.exit()
    else:
        print('API connection request success!')
        return api

"""
    REQUEST FOR DATA AND INSERT INTO A DICTIONARY
"""


def top_sub(api):
    dict = {"title": [],
            "subreddit": [],
            "score": [],
            "id": [],
            "url": [],
            "comms_num": [],
            "created": []
            }
    
    today_start=dt.datetime.today() - timedelta(days=1)
    start_epoch=int(today_start.timestamp())

    for submission in api.search_submissions(after=start_epoch,
                                subreddit='dubai',
                                filter=['title','subreddit', 'score', 'id', 'url', 'comms_num', 'created'],
                                limit=100
                                ):
        dict["title"].append(emoji.demojize(submission.title))
        dict['subreddit'].append(submission.subreddit)
        dict["score"].append(submission.score)
        dict["id"].append(submission.id)
        dict["url"].append(submission.url)
        dict["comms_num"].append(submission.num_comments)
        dict["created"].append(dt.datetime.fromtimestamp(
            submission.created).strftime('%Y-%m-%d'))

    data = dict

    return data


"""
    OUTPUT DATA TO CSV & READ FILE
"""


def top_data(data):
    top_ten = pd.DataFrame(data)
    top_ten.to_csv(
        r"/home/mahdi_mostafa_2012/git/gcp-project/output/raw_daily.csv", index=False)
    file = "/home/mahdi_mostafa_2012/git/gcp-project/output/raw_daily.csv"

    return file


"""
    UPLOAD FILE TO S3
"""


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    bucket_name = bucket_name
    source_file_name = source_file_name
    destination_blob_name = destination_blob_name

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    try:
        blob.upload_from_filename(source_file_name)
    except HTTPError as http_err:
        print('HTTP error occurred: {}'.format(http_err))
        print('System abort!')
        sys.exit()
    except Exception as e:
        print('Failed to upload data from location.')
        print('System abort!')
        print(str(e))
        sys.exit()
    else:
        print(
            "File {} has successfully uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )


"""
    LOAD DATA TO DESTINATION TABLE
"""


def load_data():
    client = bigquery.Client()
    dataset_id = 'landing'
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("subreddit", "STRING"),
        bigquery.SchemaField("score", "Integer"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("comms_num", "Integer"),
        bigquery.SchemaField("created", "DATE"),
    ]
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://dubai_source_api/source-api"

    # API request
    try:
        load_job = client.load_table_from_uri(
            uri, dataset_ref.table("dubai_posts"), job_config=job_config
        )
    except Exception as e:
        print('Failed to load data due to an expected error.')
        print('System abort!')
        print(str(e))
        sys.exit()
    else:
        print("Starting job {}".format(load_job.job_id))

    # Waits for table load to complete.
    load_job.result()
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("dubai_posts"))
    table = "landing.dubai_posts"
    print("Loaded {} rows into {}.".format(destination_table.num_rows, table))


def main():

    try:
        api = red_connect()
        data = top_sub(api)
        source_file_name = top_data(data)
        bucket_name = "dubai_source_api"
        destination_blob_name = "source-api"
        upload_blob(bucket_name, source_file_name, destination_blob_name)
        load_data()
    except Exception as e:
        print('Failed to execute statements due to an expected error.')
        print('System abort!')
        print(str(e))
        sys.exit()
    else:
        print("Load Success!")


if __name__ == "__main__":
    main()
