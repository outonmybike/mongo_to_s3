import config
import pandas as pd
from datetime import datetime

LOADED_AT = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S")
SPARK = config.generate_spark()
MDB_CLIENT = config.generate_mdb_client()
TABLE_LIST = config.table_list

MDB_DATABASE = config.MDB_DATABASE
S3_DATABASE = config.S3_DATABASE


def fetch_and_load_data(table_name,date_cols):
    db = MDB_CLIENT[f'{MDB_DATABASE}']
    collection = db[f'{table_name}']
    output = list(collection.find({}, {'_id':False}))
    df = pd.DataFrame(output)
    df['x_loaded_at'] = LOADED_AT
    for col in df.columns:
        if col not in date_cols:
            df[f'{col}'] = df[f'{col}'].astype(str)
        elif col in date_cols:
            df[f'{col}'] = pd.to_datetime(df[f'{col}'])
        else:
            print(f'error with column formatting column: {col}')
    print(f'{table_name} fetched as df from mongo')
    s3_path = f"{S3_DATABASE}.{table_name}"
    schema = SPARK.table(f"{s3_path}").schema
    s_df = SPARK.createDataFrame(df,schema)
    
    upsert_sql = config.gen_merge_sql(s_df,s3_path)
    SPARK.sql(upsert_sql)
    print(f'{table_name} written to s3')
    return

def main():
    SPARK.sql(f"CREATE DATABASE IF NOT EXISTS {S3_DATABASE}")
    for table in TABLE_LIST:
        cols = table['table_cols']
        table_name = table['table_name']
        partition_col = table['partition']
        date_cols = table['date_cols']
        config.create_schemas(table_name,cols,partition_col,SPARK)
        fetch_and_load_data(table_name,date_cols)

if __name__ == '__main__':
    main()


