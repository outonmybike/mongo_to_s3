import config
import json

MDB_CLIENT = config.generate_mdb_client()
MDB_DATABASE = config.MDB_DATABASE
TABLE_LIST = config.table_list

def main():
    # clear db if it already exists for seeding 
    MDB_CLIENT.drop_database(f'{MDB_DATABASE}')
    db = MDB_CLIENT[f'{MDB_DATABASE}']

    for table in TABLE_LIST:
        table_name = table['table_name']
        collection = db[f"{table_name}"]
        print(f'loading {table_name} seed file....')
        try:
            with open(f'mongo_seed_data/{table_name}.json', 'r') as f:
                data = json.load(f)
        except:
            print('error finding file')
            continue
        row_count = (len(data))
        if row_count == 0:
            print('empty seed file')
            continue
        elif row_count == 1:
            collection.insert_one(data)
            print('single row written')
            pass
        elif row_count > 1:
            collection.insert_many(data)
            print(f'{row_count} rows written')
            pass
        else:
            print('error with file size')
            continue
    return

if __name__ == '__main__':
    main()