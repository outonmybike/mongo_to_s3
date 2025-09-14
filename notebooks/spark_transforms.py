import config


TABLE_LIST = config.transform_list
SPARK = config.generate_spark()

S3_DATABASE = config.S3_DATABASE


def transform_s3_to_s3(table_name,transform_sql):
    s3_path = f"{S3_DATABASE}.{table_name}"
    df = SPARK.sql(transform_sql)
    upsert_sql = config.gen_merge_sql(df,s3_path)
    SPARK.sql(upsert_sql)
    print(f'{table_name} written to s3')


def main():
    for table in TABLE_LIST:
        cols = table['table_cols']
        table_name = table['table_name']
        partition_col = table['partition']
        transform_sql = table['transform_sql']
        config.create_schemas(table_name,cols,partition_col,SPARK)
        transform_s3_to_s3(table_name,transform_sql)
    return


if __name__ == "__main__":
    main()