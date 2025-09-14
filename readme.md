MongoDB to S3 Pipeline

To run the pipeline, the appropriate resources need to be spun up using docker. Docker desktop must be installed on the machine running the application. 

The docker container can be built by running the following command inside the /mongo_s3_sql directory:

```docker-compose up```

Once the container is built and the resources are spun up, they can each be viewed with a web browser on the local machine at the following paths:

Jupyter Notebook: http://localhost:8888/notebooks/notebooks/exec.ipynb

MongoDB Express: http://localhost:8081/

MinIO: http://localhost:9001/

    MinIO creds:
    username = admin
    password = password

To run the pipeline, navigate to the Jupyter notebook to execute each of the steps. 
Each step will execute a python script that in a production environment would be triggered by an orchestration tool (such as Airflow, Dagster)

Step 1:
Installs the pyspark and pymongo packages in case they aren't already available in the environment

Step 2: 
Seeds the MongoDB instance with the sample files provided. Additional lines of data were added for testing

Step 3:
Starts the first step of the ELT pipeline by first creating schemas with Iceberg formatting in the S3 environment
After that, the data for each collection is pulled by a spark into a dataframe where some metadata (x_loaded_at) is added
Finally the data is written to S3 as an Iceberg table with any partitions specified in the config.py file of the repo

Step 4:
Transformations are then run on the data as specified in the config.py file
For this project there are two transformations
The first transformation breaks out the individual records in the "lines" column of the 'journal_entries' table into rows
The second transformation calculates total debits/credits for each entry and provides a difference for quick analysis/QA

The next series of jupyter notebook cells are "SELECT *" statements to validate the tables have been built

Step 5:
Finally, commands are given to optimize the storage of the data with Iceberg compaction functions
The data retention policy (get rid of snapshots older than 30 days) is also executed in this step



Take Home Assessment Notes and considerations:

- MinIO was used as a replacement for S3. It behaves identically and allows for local testing and development

- The pipeline currently grabs all data from MongoDB with each run. However, in a production setting there would be logic baked in that grabbed files with a last updated date since the last run, or some other filter to prevent a full pull each time.

- No partition columns were added to the data as Iceberg allows parsing from existing timestamp columns

- As far as query engines, the one I would recommend would be Snowflake. 
- It integrates well with data lake structures (especially Iceberg) 
- Has a very intuitive UI easy for analysts and data scientists to access. Even allows for notebooks. 
- It allows for quick visualizations of data with the newer Snowsight UI to spot anomalies and see trends without having to push it to a BI tool
- Offers many options for secure data sharing with people outside the org
- Cloud based infrastructure means less to maintain
- And the biggest reason I would suggest it is because honestly it's the one I am most familiar with. I've spun instances up from scratch and been the primary admin at multiple jobs. 


Credits
The docker-compose file was based on the work from this github repository:
https://github.com/databricks/docker-spark-iceberg/blob/main/docker-compose.yml
