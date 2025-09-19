# hole_in_one_pipeline

This folder defines all source code for the 'hole_in_one_pipeline' pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations`: All dataset definitions and transformations.
- `utilities`: Utility functions and Python modules used in this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample under "sample_trips_hole_in_one_pipeline.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* Use `Run file` to run and preview a single transformation.
* Use `Run pipeline` to run _all_ transformations in the entire pipeline.
* Use `+ Add` in the file browser to add a new data set definition.
* Use `Schedule` to run the pipeline on a schedule!

For more tutorials and reference material, see https://docs.databricks.com/dlt.

# Reading JSON Data from outside of Databricks via API

Successful read of json: (Must update link based on databricks and add auth bearer token in postman / the request)
Must also create an authorization token in DataBricks UI

https://dbc-394e6439-48e4.cloud.databricks.com/api/2.0/fs/files/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume/data_bronze.json

https://dbc-394e6439-48e4.cloud.databricks.com/api/2.0/fs/files/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume/data_silver.json

https://dbc-394e6439-48e4.cloud.databricks.com/api/2.0/fs/files/Volumes/hole_in_one/hole_in_one_schema/hole_in_one_volume/data_gold.json

### References:

https://www.chaosgenius.io/blog/databricks-jobs-api/ 