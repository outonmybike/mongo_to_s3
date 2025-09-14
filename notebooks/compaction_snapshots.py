
import config
from datetime import datetime, timedelta

SPARK = config.generate_spark()
S3_DATABASE = config.S3_DATABASE
DAYS_TO_RETAIN = 30

prev_date = datetime.now() - timedelta(days=DAYS_TO_RETAIN)
expire_date = prev_date.strftime('%Y-%m-%d %H:%M:%S')

def compaction_and_snapshot_expire():
    df = SPARK.sql(f"show tables in {S3_DATABASE}")
    table_list = df.select("tableName").rdd.map(lambda row: row[0]).collect()
    for table in table_list:
        print(f'####### running compaction on table: {table} ##########')
        spark_sql = f"CALL system.rewrite_data_files(table => '{S3_DATABASE}.{table}')"
        cdf = SPARK.sql(spark_sql)
        cdf.show()
        print(f'####### dropping old snapshots on table: {table} ##########')
        full_table = f"{S3_DATABASE}.{table}"
        snap_sql = f"CALL system.expire_snapshots(table => '{full_table}', older_than => TIMESTAMP '{expire_date}')"
        sdf = SPARK.sql(snap_sql)
        sdf.show()

if __name__ == "__main__":
    compaction_and_snapshot_expire()

