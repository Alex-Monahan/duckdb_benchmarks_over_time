import pandas as pd
import duckdb
import json
import os 
import time
import shutil
import sys
from pathlib import Path

from SQLiteLogger import SQLiteLogger

def delete_database(filename):
    db_filename = filename + '.duckdb'
    try:
        os.remove(db_filename)
    except OSError:
        pass
    
    wal_filename = filename + '.duckdb.wal'
    try:
        os.remove(wal_filename)
    except OSError:
        pass

    tmp_filename = filename + '.duckdb.tmp'
    try:
        shutil.rmtree(tmp_filename)
    except OSError:
        pass

def time_and_log(f, *args, **kwargs):
    """Psuedo decorator for timing and logging.
    r, b, s, and l are special kwargs for logging purposes.
    Called like: time_and_log(sleepy,0.3,time_to_sleep_kw=0.5, r=1, b='007.4 Export group by results to Arrow', s=json.dumps({'duckdb_version':duckdb_version}), l=logger)"""
    # Logger schema for reference
    # run_id int, -- Auto-generated when logger is instantiated
    # repeat_id int,
    # benchmark varchar,
    # scenario json,
    # time float 
    def wrapped_func(*args, **kwargs):
        start_time = time.perf_counter()
        # Exclude repeat_id, benchmark, scenario, logger
        trimmed_kwargs = {k:kwargs.get(k) for k in kwargs if k not in ['r', 'b', 's', 'l'] }
        result = f(*args, **trimmed_kwargs)
        end_time = time.perf_counter()
        kwargs.get('l').log([(kwargs.get('r'),kwargs.get('b'),kwargs.get('s'),(end_time - start_time))])
        return result
    return wrapped_func(*args, **kwargs)

def get_duckdb_version_and_scenario():
    con = duckdb.connect(':memory:')
    duckdb_version = con.execute('select version()').fetchall()[0][0]
    print(duckdb_version)
    scenario = json.dumps({'duckdb_version':duckdb_version})
    con.close()
    return duckdb_version, scenario

def connect_to_duckdb(venv_location, duckdb_version):
    db_filepath = venv_location+'/'+duckdb_version.replace('.','_')
    delete_database(db_filepath)

    con = duckdb.connect(db_filepath+'.duckdb')
    temp_dir = venv_location+'/tmp'
    
    try:
        shutil.rmtree(temp_dir)
    except OSError:
        pass

    Path(temp_dir).mkdir()

    print(con.execute(f"pragma temp_directory='{temp_dir}'").fetchall())
    return con 

def pandas_test(con):
    my_df = pd.DataFrame.from_dict({'a': [42]})
    return con.execute("select * from my_df").df()

def ingest_group_by_csv(con, csv_file, duckdb_version, versions_without_enums):
    table_name = 'y'

    # If Enum from query support is not present, insert directly into table x
    if duckdb_version in versions_without_enums:
        table_name = 'x'
    create_table_queries = [
        f"DROP TABLE IF EXISTS {table_name}",
        f"CREATE TABLE {table_name}(id1 VARCHAR, id2 VARCHAR, id3 VARCHAR, id4 INT, id5 INT, id6 INT, v1 INT, v2 INT, v3 FLOAT)",
        f"COPY {table_name} FROM '{csv_file}' (AUTO_DETECT TRUE)",
        "CHECKPOINT",
    ]
    for query in create_table_queries:
        con.execute(query).fetchall()

def convert_to_enums_group_by(con, duckdb_version):
    convert_to_enum_queries = [
        # if there are no nulls (which duckdb enums can't handle, make enums)
        "DROP TYPE IF EXISTS id1ENUM",
        "DROP TYPE IF EXISTS id2ENUM",
        "CREATE TYPE id1ENUM AS ENUM (SELECT id1 FROM y)",
        "CREATE TYPE id2ENUM AS ENUM (SELECT id2 FROM y)",
        "DROP TABLE IF EXISTS x",
        "CREATE TABLE x(id1 id1ENUM, id2 id2ENUM, id3 VARCHAR, id4 INT, id5 INT, id6 INT, v1 INT, v2 INT, v3 FLOAT)",
        "INSERT INTO x (SELECT * FROM y)",
        "DROP TABLE IF EXISTS y",
        "CHECKPOINT",
    ]
    for query in convert_to_enum_queries:
        con.execute(query).fetchall()

def group_by_queries(con):
    # From 33 seconds in 0.2.7 to 1.5 seconds in 0.10!
    # Using bigint instead of hugeint due to older parquet writer issues 
    group_by_queries = [
        "DROP TABLE IF EXISTS ans01",
        "DROP TABLE IF EXISTS ans02",
        "DROP TABLE IF EXISTS ans03",
        "DROP TABLE IF EXISTS ans04",
        "DROP TABLE IF EXISTS ans05",
        "DROP TABLE IF EXISTS ans06",
        "DROP TABLE IF EXISTS ans07",
        "DROP TABLE IF EXISTS ans08",
        "DROP TABLE IF EXISTS ans09",
        "DROP TABLE IF EXISTS ans10",
        "CREATE TABLE ans01 AS SELECT id1, sum(v1)::bigint AS v1 FROM x GROUP BY id1",
        "CREATE TABLE ans02 AS SELECT id1, id2, sum(v1)::bigint AS v1 FROM x GROUP BY id1, id2",
        "CREATE TABLE ans03 AS SELECT id3, sum(v1)::bigint AS v1, avg(v3) AS v3 FROM x GROUP BY id3",
        "CREATE TABLE ans04 AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4",
        "CREATE TABLE ans05 AS SELECT id6, sum(v1)::bigint AS v1, sum(v2)::bigint AS v2, sum(v3)::bigint AS v3 FROM x GROUP BY id6",
        "CREATE TABLE ans06 AS SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM x GROUP BY id4, id5",
        "CREATE TABLE ans07 AS SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM x GROUP BY id3",
        "CREATE TABLE ans08 AS SELECT id6, v3 AS largest2_v3 FROM (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2",
        "CREATE TABLE ans09 AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4",
        "CREATE TABLE ans10 AS SELECT id1, id2, id3, id4, id5, id6, sum(v3)::bigint AS v3, count(*)::bigint AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6",
        "CHECKPOINT",
    ]
    print('Beginning group by queries')
    for query in group_by_queries:
        con.execute(query).fetchall()

def export_group_by_to_pandas(con):
    # Export group by results to Pandas (from 4.8 seconds in 0.2.7 to 1.3 seconds in 0.10)
    # (exporting over 10 million rows)
    for r in range(1, 11):
        result_table = "ans"+str(r).zfill(2)
        pandas_df = con.execute(f"select * from {result_table}").fetch_df()
        print(result_table, len(pandas_df), "rows")
    # Return the final df for next step
    return pandas_df

def read_pandas(con, pandas_df):
    # Read from a 10,000,000 row Pandas dataframe (from 0.45 seconds in 0.2.7 to 0.008 seconds in 0.10)
    pandas_df_summary = con.execute("select sum(v3) as v3 from pandas_df").fetch_df()

def export_group_by_to_parquet(con, venv_location):
    # Write out group by results to parquet (from 2.5 seconds to 1.8 seconds in 0.10)
    for r in range(1, 11):
        result_table = "ans"+str(r).zfill(2)
        parquet_file = str(Path(venv_location).parent) + f'/_data/group_by_{result_table}.parquet'
        parquet_output = con.execute(f"COPY {result_table} to '{parquet_file}' (FORMAT PARQUET)").fetch_df()
    # Return the final parquet file for next step
    return parquet_file

def read_parquet(con, parquet_file):
    # Read from a 10,000,000 row Parquet file (ans10) (from 0.11 seconds to 0.014 in 0.10)
    parquet_summary = con.execute(f"select sum(v3) as v3 from '{parquet_file}'").fetch_df()

def export_group_by_to_arrow(con):
    # Export group by results to Arrow (from  seconds in 0.2.8 to  seconds in 0.10)
    # (exporting over 10 million rows)
    for r in range(1, 11):
        result_table = "ans"+str(r).zfill(2)
        arrow_df = con.execute(f"select * from {result_table}").fetch_arrow_table()
    # Return the final arrow_df for the next step
    return arrow_df

def read_arrow(con, arrow_df):
    # Read from a 10,000,000 row Pandas dataframe (from 0.45 seconds in 0.2.7 to 0.008 seconds in 0.10)
    arrow_df_summary = con.execute("select sum(v3) as v3 from arrow_df").fetch_arrow_table()

def ingest_join_csvs(con, x_csv, small_csv, medium_csv, big_csv, duckdb_version, versions_without_enums):
    # Load data for join queries (10.4 seconds to 3.4 seconds)
    table_suffix = '_csv'
    if duckdb_version in versions_without_enums:
        table_suffix = ''
    create_table_queries_joins = [
        f"DROP TABLE IF EXISTS x{table_suffix}",
        f"DROP TABLE IF EXISTS small{table_suffix}",
        f"DROP TABLE IF EXISTS medium{table_suffix}",
        f"DROP TABLE IF EXISTS big{table_suffix}",
        f"CREATE TABLE x{table_suffix} AS SELECT * FROM read_csv_auto('{x_csv}')",
        f"CREATE TABLE small{table_suffix} AS SELECT * FROM read_csv_auto('{small_csv}')",
        f"CREATE TABLE medium{table_suffix} AS SELECT * FROM read_csv_auto('{medium_csv}')",
        f"CREATE TABLE big{table_suffix} AS SELECT * FROM read_csv_auto('{big_csv}')",
        "CHECKPOINT"
    ]

    print('Beginning create_table_queries_joins', duckdb_version)
    for query in create_table_queries_joins:
        con.execute(query).fetchall()

def convert_to_enums_joins(con):
    id4_enum_statement = "SELECT id4 FROM x_csv UNION ALL SELECT id4 FROM small_csv UNION ALL SELECT id4 from medium_csv UNION ALL SELECT id4 from big_csv"
    id5_enum_statement = "SELECT id5 FROM x_csv UNION ALL SELECT id5 from medium_csv UNION ALL SELECT id5 from big_csv"
    convert_to_enum_queries_joins = [
        "DROP TYPE IF EXISTS id4ENUM",
        "DROP TYPE IF EXISTS id5ENUM",
        f"CREATE TYPE id4ENUM AS ENUM ({id4_enum_statement})",
        f"CREATE TYPE id5ENUM AS ENUM ({id5_enum_statement})",

        "DROP TABLE IF EXISTS small",
        "CREATE TABLE small(id1 INT64, id4 id4ENUM, v2 DOUBLE)",
        "INSERT INTO small (SELECT * from small_csv)",

        "DROP TABLE IF EXISTS medium",
        "CREATE TABLE medium(id1 INT64, id2 INT64, id4 id4ENUM, id5 id5ENUM, v2 DOUBLE)",
        "INSERT INTO medium (SELECT * FROM medium_csv)",

        "DROP TABLE IF EXISTS big",
        "CREATE TABLE big(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, id6 VARCHAR, v2 DOUBLE)",
        "INSERT INTO big (Select * from big_csv)",

        "DROP TABLE IF EXISTS x",
        "CREATE TABLE x(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, id6 VARCHAR, v1 DOUBLE)",
        "INSERT INTO x (SELECT * FROM x_csv)",

        # drop all the csv ingested tables
        "DROP TABLE x_csv",
        "DROP TABLE small_csv",
        "DROP TABLE medium_csv",
        "DROP TABLE big_csv",
        "CHECKPOINT"
    ]
    for query in convert_to_enum_queries_joins:
        con.execute(query).fetchall()

def join_queries(con):
    # Join queries from 28.5 seconds to 4.1 seconds
    join_queries = [
        "DROP TABLE IF EXISTS ans1",
        "DROP TABLE IF EXISTS ans2",
        "DROP TABLE IF EXISTS ans3",
        "DROP TABLE IF EXISTS ans4",
        "DROP TABLE IF EXISTS ans5",
        "CREATE TABLE ans1 AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)",
        "CREATE TABLE ans2 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)",
        "CREATE TABLE ans3 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)",
        "CREATE TABLE ans4 AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)",
        "CREATE TABLE ans5 AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)",
        "CHECKPOINT",
    ]
    
    for query in join_queries:
        con.execute(query).fetchall()

def export_join_results_to_pandas(con):
    # Export join results to Pandas from 47 seconds to 10 seconds
    for r in range(1, 6):
        result_table = "ans"+str(r)
        pandas_df = con.execute(f"select * from {result_table}").fetch_df()

def export_join_to_arrow(con):
    # Export join results to Arrow from  seconds to  seconds
    for r in range(1, 6):
        result_table = "ans"+str(r)
        arrow_df = con.execute(f"select * from {result_table}").fetch_arrow_table()

# This needs to match the filename in the calling loop
logger = SQLiteLogger('benchmark_log_python.db', delete_file=False)

repeat = 3
versions_without_enums = ['0.2.7', '0.2.8', '0.2.9', '0.3.0', '0.3.1', '0.3.2', '0.3.3', '0.3.4', '0.4.0', '0.5.1']
versions_without_pyarrow = ['0.2.7', '0.2.8', '0.2.9', '0.3.0']

for i in range(repeat):
    try:
        duckdb_version, scenario = get_duckdb_version_and_scenario()

        venv_location = str(Path(sys.executable).parent.parent)
        
        con = connect_to_duckdb(venv_location, duckdb_version)
        
        time_and_log(pandas_test, con, 
                     r=i, b='001 Query pandas', s=scenario, l=logger)

        csv_file = str(Path(venv_location).parent) + '/_data/G1_1e7_1e2_0_0.csv'
        time_and_log(ingest_group_by_csv, con, csv_file, duckdb_version, versions_without_enums,
                     r=i, b='002 Create table from csv', s=scenario, l=logger)
        
        if duckdb_version not in versions_without_enums:
            time_and_log(convert_to_enums_group_by, con, duckdb_version,
                        r=i, b='003 Convert to Enums', s=scenario, l=logger)

        time_and_log(group_by_queries, con,
                     r=i, b='004 Group by queries', s=scenario, l=logger)

        pandas_df = time_and_log(export_group_by_to_pandas, con,
                     r=i, b='005 Export group by results to Pandas', s=scenario, l=logger)

        time_and_log(read_pandas, con, pandas_df,
                     r=i, b='006 Scan and aggregate over Pandas df', s=scenario, l=logger)

        parquet_file = time_and_log(export_group_by_to_parquet, con, venv_location,
                     r=i, b='007 Export group by results to Parquet', s=scenario, l=logger)

        time_and_log(read_parquet, con, parquet_file,
                     r=i, b='008 Scan and aggregate over Parquet file', s=scenario, l=logger)

        # Skip pyarrow tests on version 0.2.7-0.3.0 since numpy wouldn't compile correctly on M1 Mac
        if not duckdb_version in versions_without_pyarrow: 
            import pyarrow
            arrow_df = time_and_log(export_group_by_to_arrow, con,
                     r=i, b='009 Export group by results to Arrow', s=scenario, l=logger)
            
            time_and_log(read_arrow, con, arrow_df,
                     r=i, b='010 Scan and aggregate over Arrow df', s=scenario, l=logger)

        x_csv = str(Path(venv_location).parent) + '/_data/J1_1e7_NA_0_0.csv'
        small_csv = str(Path(venv_location).parent) + '/_data/J1_1e7_1e1_0_0.csv'
        medium_csv = str(Path(venv_location).parent) + '/_data/J1_1e7_1e4_0_0.csv'
        big_csv = str(Path(venv_location).parent) + '/_data/J1_1e7_1e7_0_0.csv'
        time_and_log(ingest_join_csvs, con, x_csv, small_csv, medium_csv, big_csv, duckdb_version, versions_without_enums,
                     r=i, b='011 Create tables from csvs joins', s=scenario, l=logger)

        if duckdb_version not in versions_without_enums:
            time_and_log(convert_to_enums_joins, con,
                     r=i, b='012 Convert to Enums for joins', s=scenario, l=logger)

        time_and_log(join_queries, con,
                     r=i, b='013 Join queries', s=scenario, l=logger)

        time_and_log(export_join_results_to_pandas, con,
                     r=i, b='014 Export join results to Pandas', s=scenario, l=logger)

        # Skip pyarrow tests on version 0.2.7-0.3.0 since numpy wouldn't compile correctly
        if not duckdb_version in versions_without_pyarrow: 
            time_and_log(export_join_to_arrow, con,
                     r=i, b='015 Export join results to Arrow', s=scenario, l=logger)

    except Exception as err:
        import traceback
        print("ERROR in duckdb_version",duckdb_version)
        print(err)
        print(traceback.print_exc())
    finally:
        con.close()

# Group by - see if we OOM!
csv_files = [
    str(Path(venv_location).parent) + '/_data/G1_1e8_1e2_0_0.csv',
    str(Path(venv_location).parent) + '/_data/G1_1e9_1e2_0_0.csv'
]
for csv_file in csv_files:
    try:
        con = connect_to_duckdb(venv_location, duckdb_version)
        row_count = csv_file.split('/')[-1].split('_')[1]
        scenario = json.dumps({'duckdb_version':duckdb_version, "row_count":row_count})
        time_and_log(ingest_group_by_csv, con, csv_file, duckdb_version, versions_without_enums,
                    r=i, b='101 Group By Scale test: Create table from csv', s=scenario, l=logger)
        
        if duckdb_version not in versions_without_enums:
            time_and_log(convert_to_enums_group_by, con, duckdb_version,
                        r=i, b='102 Group By Scale test: Convert to Enums', s=scenario, l=logger)

        time_and_log(group_by_queries, con,
                    r=i, b='103 Group By Scale test: Group by queries', s=scenario, l=logger)
    
    except Exception as err:
        import traceback
        print("ERROR in duckdb_version", duckdb_version, 'row_count', row_count)
        print(err)
        print(traceback.print_exc())
        # No need to try a larger file if the other failed already
        break
    finally:
        con.close()

# Join - see if we OOM!
join_csv_files = [
    {
        'x_csv' :str(Path(venv_location).parent) + '/_data/J1_1e8_NA_0_0.csv',
        'small_csv': str(Path(venv_location).parent) + '/_data/J1_1e8_1e2_0_0.csv',
        'medium_csv': str(Path(venv_location).parent) + '/_data/J1_1e8_1e5_0_0.csv',
        'big_csv': str(Path(venv_location).parent) + '/_data/J1_1e8_1e8_0_0.csv',
    },
    {
        'x_csv' :str(Path(venv_location).parent) + '/_data/J1_1e9_NA_0_0.csv',
        'small_csv': str(Path(venv_location).parent) + '/_data/J1_1e9_1e3_0_0.csv',
        'medium_csv': str(Path(venv_location).parent) + '/_data/J1_1e9_1e6_0_0.csv',
        'big_csv': str(Path(venv_location).parent) + '/_data/J1_1e9_1e9_0_0.csv',
    },
]
for csv_file_dict in join_csv_files:
    try:
        con = connect_to_duckdb(venv_location, duckdb_version)
        x_csv = csv_file_dict['x_csv']
        small_csv = csv_file_dict['small_csv']
        medium_csv = csv_file_dict['medium_csv']
        big_csv = csv_file_dict['big_csv']

        row_count = x_csv.split('/')[-1].split('_')[1]
        scenario = json.dumps({'duckdb_version':duckdb_version, "row_count":row_count})

        time_and_log(ingest_join_csvs, con, x_csv, small_csv, medium_csv, big_csv, duckdb_version, versions_without_enums,
                     r=i, b='201 Join Scale test: Create tables from csvs joins', s=scenario, l=logger)

        if duckdb_version not in versions_without_enums:
            time_and_log(convert_to_enums_joins, con,
                     r=i, b='202 Join Scale test: Convert to Enums for joins', s=scenario, l=logger)

        time_and_log(join_queries, con,
                     r=i, b='203 Join Scale test: Join queries', s=scenario, l=logger)
    
    except Exception as err:
        import traceback
        print("ERROR in duckdb_version", duckdb_version, 'row_count', row_count)
        print(err)
        print(traceback.print_exc())
        # No need to try a larger file if the other failed already
        break
    finally:
        con.close()

# TODO: S3 Parquet reader / writer? 

