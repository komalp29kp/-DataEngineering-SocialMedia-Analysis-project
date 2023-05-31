import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
arguments = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark_session = glue_context.spark_session
glue_job = Job(glue_context)
glue_job.init(arguments['JOB_NAME'], arguments)

region_filter = "region in ('ca','gb','us')"

source_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database="db_youtube_raw", table_name="raw_statistics", transformation_ctx="source_dynamic_frame", push_down_predicate=region_filter)

apply_mapping_frame = ApplyMapping.apply(frame=source_dynamic_frame, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "long"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "long"), ("likes", "long", "likes", "long"), ("dislikes", "long", "dislikes", "long"), ("comment_count", "long", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "boolean"), ("ratings_disabled", "boolean", "ratings_disabled", "boolean"), ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx="apply_mapping_frame")

resolve_choice_frame = ResolveChoice.apply(frame=apply_mapping_frame, choice="make_struct", transformation_ctx="resolve_choice_frame")

drop_null_fields_frame = DropNullFields.apply(frame=resolve_choice_frame, transformation_ctx="drop_null_fields_frame")

sink_data_frame = drop_null_fields_frame.toDF().coalesce(1)
dynamic_frame_final_output = DynamicFrame.fromDF(sink_data_frame, glue_context, "dynamic_frame_final_output")
sink_options = {"path": "s3://de-on-youtube-cleansed-useast1-dev/youtube/raw_statistics/", "partitionKeys": ["region"]}
sink_data_frame = glue_context.write_dynamic_frame.from_options(frame=dynamic_frame_final_output, connection_type="s3", connection_options=sink_options, format="parquet", transformation_ctx="sink_data_frame")

glue_job.commit()
