import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        # Read JSON content and create DataFrame
        raw_data_df = wr.s3.read_json('s3://{}/{}'.format(input_bucket, input_key))

        # Extract required columns:
        processed_data_df = pd.json_normalize(raw_data_df['items'])

        # Write processed data to S3 as Parquet format
        wr_response = wr.s3.to_parquet(
            df=processed_data_df,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(input_key, input_bucket))
        raise e
