import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'])

bucket_name = "de-on-youtubedata-cleaned-athenajob"

# Copy JSON Reference data to the specified location
json_reference_location = "s3://{}/youtube/raw_statistics_reference_data/".format(bucket_name)
json_reference_command = "aws s3 cp . {} --recursive --exclude '*' --include '*.json'".format(json_reference_location)
os.system(json_reference_command)

# Copy data files to their respective locations
data_files = {
    "CAvideos.csv": "ca",
    "DEvideos.csv": "de",
    "FRvideos.csv": "fr",
    "GBvideos.csv": "gb",
    "INvideos.csv": "in",
    "JPvideos.csv": "jp",
    "KRvideos.csv": "kr",
    "MXvideos.csv": "mx",
    "RUvideos.csv": "ru",
    "USvideos.csv": "us"
}

for file_name, region in data_files.items():
    file_location = "s3://{}/youtube/raw_statistics/region={}/{}".format(bucket_name, region, file_name)
    file_command = "aws s3 cp {} {}".format(file_name, file_location)
    os.system(file_command)

job.commit()
