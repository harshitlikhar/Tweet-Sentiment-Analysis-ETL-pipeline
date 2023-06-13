from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
import tweepy
import boto3

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'email': ['me@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'REDSHIFT_CONNECTION': 'your-redshift-connection-name',
    'REDSHIFT_TABLE': 'your-redshift-table-name'
}

dag = DAG(
    'twitter_to_s3',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
)

def collect_tweet_data():
    # Authenticate to Twitter API
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Create API object
    api = tweepy.API(auth)

    # Collect tweets
    tweets = api.search(q='keyword', lang='en', count=100)

    # Store tweets in S3
    s3 = boto3.resource('s3')
    s3.Object('my-s3-bucket', 'tweets.json').put(Body=str(tweets))

with dag:
    collect_tweet_data_task = PythonOperator(
        task_id='collect_tweet_data',
        python_callable=collect_tweet_data,
    )

    s3_sensor_task = S3KeySensor(
        task_id='s3_sensor',
        bucket_key='your-bucket/path/to/data/*.json',  # Adjust the S3 key pattern
        wildcard_match=True,
    )

    glue_operator_task = AwsGlueJobOperator(
        task_id='glue_job',
        job_name='your-glue-job-name',
        script_location='s3://your-glue-scripts/script.py',
        script_args={
            '--s3_input': '{{ ti.xcom_pull(task_ids="s3_sensor") }}'  # Use ti.xcom_pull instead of task_instance.xcom_pull
        },
    )

    collect_tweet_data_task >> s3_sensor_task >> glue_operator_task  # Set task dependencies
