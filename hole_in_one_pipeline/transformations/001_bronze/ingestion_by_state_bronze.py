import requests
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

json_schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])

dbutils.fs.mkdirs("/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume")

# Get hole in one data by state from API
api_url = "https://admin.nationalholeinoneregistry.com/nhio-micro/registrations?course_state=Rhode Island"
response = requests.get(api_url)
data = response.json()

try:
    dbutils.fs.put("/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume/data_bronze.json", str(data['registrations']), overwrite=True)
except Exception as e:
    print("Error:", e)


@dlt.table(
    name="hole_in_one_bronze",
    comment="Raw JSON data ingested from source location.",
    table_properties={"quality": "bronze"}
)
def hole_in_one_bronze():
    """
    Ingests raw JSON data into a streaming table using Auto Loader.
    """
    json_path = "dbfs:/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume"

    df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .schema(json_schema) \
    .load(json_path)

    return df







