# Import required libraries
from pyspark.sql import SparkSession
from pymongo import MongoClient
import os


MDB_DATABASE = 'accounting_db'
S3_DATABASE = 'mongodb_imports'

cwd = os.getcwd()
ENV = 'nb' if 'iceberg' in cwd else 'vsc'

def generate_mdb_client():
    path = 'localhost:27017' if ENV == 'vsc' else 'mongo'
    client = MongoClient(f'mongodb://root:example@{path}/admin')
    return client


def generate_spark():
    spark = SparkSession.builder \
        .appName("IcebergTableCreation") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .getOrCreate()
    return spark

############################################################
accounts_cols = [
    'account_id string',
    'name string',
    'type string',
    'x_loaded_at string',
]

journal_entries_cols = [
    'entry_id string',
    'date date',
    'lines string',
    'description string',
    'x_loaded_at string',
]

close_tasks_cols = [
    'task_id string',
    'name string',
    'assigned_to string',
    'status string',
    'due_date date',
    'x_loaded_at string',
]

############################################################

table_list = [
    {
        "table_name":"accounts",
        "partition":"",
        "table_cols": accounts_cols,
        "load_freq":"all",
        "date_cols":[],
    },
    {
        "table_name":"journal_entries",
        "partition":"months(date)",
        "table_cols": journal_entries_cols,
        "load_freq":"all",
        "date_cols":['date'],
    },    
    {
        "table_name":"close_tasks",
        "partition":"",
        "table_cols": close_tasks_cols,
        "load_freq":"all",
        "date_cols":['due_date'],
    },
    ]


############################################################

def table_gen_sql(database, table_name, table_cols, partition_col):
    partition_txt = '' if partition_col == '' else f"PARTITIONED BY ({partition_col})"
    col_text = ", ".join(table_cols)
    sql = f'create table if not exists {database}.{table_name} ({col_text}) USING iceberg {partition_txt}'
    return sql


def create_schemas(table_name,cols,partition_col,spark):
    table_sql = table_gen_sql(S3_DATABASE,table_name,cols,partition_col)
    spark.sql(table_sql)
    print(f'{table_name} created in S3 as iceberg table')
    return


def gen_merge_sql(df,s3_path):
    col_matching_txt = ''
    for x in df.columns:
        col_matching_txt = col_matching_txt + f"target.{x} = source.{x}, "
    col_matching_txt = col_matching_txt[:-2]

    id_col = df.columns[0]
    df.createOrReplaceTempView("data_to_upsert")
    upsert_sql = f"""
    MERGE INTO {s3_path} as target using data_to_upsert as source
    on target.{id_col} = source.{id_col}
    WHEN MATCHED THEN 
        UPDATE SET {col_matching_txt}
    WHEN NOT MATCHED THEN
        INSERT *
    """
    return upsert_sql


############################################################
# TRANSFORMS
############################################################

journal_entries_lines_cols = [
    'line_id string',
    'entry_id string',
    'date date',
    'description string',
    'account_id string',
    'debit double',
    'credit double',
    'x_loaded_at string',
]

journal_entry_totals_cols = [
    'entry_id string',
    'debits double',
    'credits double',
    'diff double',
]


journal_lines_sql = """
with base as (
SELECT
entry_id,
date,
description,
entry.account_id AS account_id,
entry.debit AS debit,
entry.credit AS credit,
x_loaded_at
FROM (
SELECT
    entry_id,
    date,
    description,
    x_loaded_at,
from_json(lines,
'array<struct<account_id:string,debit:double,credit:double>>'
) AS json_parsed
FROM mongodb_imports.journal_entries) t
LATERAL VIEW explode(json_parsed) e AS entry
)
select 
concat(entry_id,"_",row_number() over (partition by entry_id order by debit)) as line_id,
*
from base
"""

journal_entry_totals_sql = """
select 
    entry_id,
    sum(debit) as debits,
    sum(credit) as credits,
    sum(debit)-sum(credit) as diff
    from mongodb_imports.journal_entries_lines
group by 1
"""


transform_list = [
    {
        "table_name":"journal_entries_lines",
        "transform_sql":journal_lines_sql,
        "partition":"months(date)",
        "date_cols":['date'],        
        "table_cols":journal_entries_lines_cols

    },
    {
        "table_name":"journal_entry_totals",
        "transform_sql":journal_entry_totals_sql,
        "partition":"",
        "date_cols":[],        
        "table_cols":journal_entry_totals_cols

    },    
]