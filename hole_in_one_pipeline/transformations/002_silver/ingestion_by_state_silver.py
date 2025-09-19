import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name="hole_in_one_silver",
    comment="Cleansed and validated hole in one data",
    table_properties={"quality": "silver"}
)
def hole_in_one_silver():
    # Silver Layer Transformations
    df = spark.read.option("multiline", "true").json("/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume/data_bronze.json") \
        .withColumn("first_name", upper("first_name"))
    
    columns_to_drop = [
        "age_group", "ball_used", "club_brand", "club_used", "comments", 
        "course_country", "course_id", "course_state", "createdAt", 
        "date_of_hole_in_one_date", "gender", "golfers_country", "handicap", "id",
        "number_of_hios", "regdate", "updatedAt", "witness_one", "witness_two", "witness_three", "years_golfing"                       
    ]
    df_cleaned = df.drop(*columns_to_drop)

    # Write new silver json file to Volume
    single_file_output_path = "/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume/data_silver"
    new_output_path = "/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume/data_silver.json"
    df_cleaned.coalesce(1).write.format("json").mode("overwrite").save(single_file_output_path)

    # Logic to get json into one file
    list_files = dbutils.fs.ls(single_file_output_path)
    json_file_path = [file.path for file in list_files if '.json' in file.path][0]
    dbutils.fs.mv(json_file_path, new_output_path)
    dbutils.fs.rm(single_file_output_path, True)

    return df_cleaned