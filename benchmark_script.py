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

# Logger schema for reference
# run_id int, -- Auto-generated when logger is instantiated
# repeat_id int,
# benchmark varchar,
# scenario json,
# time float 

# This needs to match the filename in the calling loop
logger = SQLiteLogger('benchmark_log_python.db', delete_file=False)

repeat = 2
versions_without_enums = ['0.2.7', '0.2.8', '0.2.9', '0.3.0', '0.3.1', '0.3.2', '0.3.3', '0.3.4', '0.4.0', '0.5.1']

for i in range(repeat):
    try:
        print(sys.executable)
        venv_location = str(Path(sys.executable).parent.parent)
        print(venv_location)
        
        start_time = time.perf_counter()
        con = duckdb.connect(':memory:')
        end_time = time.perf_counter()
        duckdb_version = con.execute('select version()').fetchall()[0][0]
        print(duckdb_version)
        con.close()
        logger.log([(i,'001 Connect in memory',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

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
        my_df = pd.DataFrame.from_dict({'a': [42]})

        start_time = time.perf_counter()
        pandas_results = con.execute("select * from my_df").df()
        print(pandas_results)
        end_time = time.perf_counter()
        logger.log([(i,'002 Query pandas',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])


        # Create the table from the csv file
            
        # first create and ingest the table.
        csv_file = str(Path(venv_location).parent) + '/_data/G1_1e7_1e2_0_0.csv'
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
        start_time = time.perf_counter()
        for query in create_table_queries:
            con.execute(query).fetchall()
        end_time = time.perf_counter()
        logger.log([(i,'003 Create table from csv',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        convert_to_enum_queries = [
            # if there are no nulls (which duckdb enums can't handle, make enums)
            "CREATE TYPE id1ENUM AS ENUM (SELECT id1 FROM y)",
            "CREATE TYPE id2ENUM AS ENUM (SELECT id2 FROM y)",
            "CREATE TABLE x(id1 id1ENUM, id2 id2ENUM, id3 VARCHAR, id4 INT, id5 INT, id6 INT, v1 INT, v2 INT, v3 FLOAT)",
            "INSERT INTO x (SELECT * FROM y)",
            "DROP TABLE IF EXISTS y",
            "CHECKPOINT",
        ]
        # If Enum from query support is not present, do not try to create any enums
        if duckdb_version not in versions_without_enums:
            start_time = time.perf_counter()
            for query in convert_to_enum_queries:
                con.execute(query).fetchall()
            end_time = time.perf_counter()
            logger.log([(i,'004 Convert to Enums',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # From 33 seconds in 0.2.7 to 1.5 seconds in 0.10!
        # Using bigint instead of hugeint due to older parquet writer issues 
        group_by_queries = [
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
        print('Beginning group by queries', duckdb_version)
        start_time = time.perf_counter()
        for query in group_by_queries:
            con.execute(query).fetchall()
        end_time = time.perf_counter()
        logger.log([(i,'005 Group by queries',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # Export group by results to Pandas (from 4.8 seconds in 0.2.7 to 1.3 seconds in 0.10)
        # (exporting over 10 million rows)
        start_time = time.perf_counter()
        for r in range(1, 11):
            result_table = "ans"+str(r).zfill(2)
            pandas_df = con.execute(f"select * from {result_table}").fetch_df()
            print(result_table, len(pandas_df), "rows")
        end_time = time.perf_counter()
        logger.log([(i,'006 Export group by results to Pandas',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # Read from a 10,000,000 row Pandas dataframe (from 0.45 seconds in 0.2.7 to 0.008 seconds in 0.10)
        start_time = time.perf_counter()
        pandas_df_summary = con.execute("select sum(v3) as v3 from pandas_df").fetch_df()
        print(pandas_df_summary)
        end_time = time.perf_counter()
        logger.log([(i,'007.1 Scan and aggregate over Pandas df',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # Write out group by results to parquet (from 2.5 seconds to 1.8 seconds in 0.10)
        start_time = time.perf_counter()
        for r in range(1, 11):
            result_table = "ans"+str(r).zfill(2)
            parquet_file = str(Path(venv_location).parent) + f'/_data/group_by_{result_table}.parquet'
            parquet_output = con.execute(f"COPY {result_table} to '{parquet_file}' (FORMAT PARQUET)").fetch_df()
            print(parquet_output)
        end_time = time.perf_counter()
        logger.log([(i,'007.2 Export group by results to Parquet',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # Read from a 10,000,000 row Parquet file (ans10) (from 0.11 seconds to 0.014 in 0.10)
        start_time = time.perf_counter()
        parquet_summary = con.execute(f"select sum(v3) as v3 from '{parquet_file}'").fetch_df()
        print(parquet_summary)
        end_time = time.perf_counter()
        logger.log([(i,'007.3 Scan and aggregate over Parquet file',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])


        # Skip pyarrow tests on version 0.2.7 since numpy wouldn't compile correctly
        if not duckdb_version == '0.2.7': 
            # Export group by results to Arrow (from  seconds in 0.2.8 to  seconds in 0.10)
            # (exporting over 10 million rows)
            import pyarrow
            start_time = time.perf_counter()
            for r in range(1, 11):
                result_table = "ans"+str(r).zfill(2)
                arrow_df = con.execute(f"select * from {result_table}").fetch_arrow_table()
            end_time = time.perf_counter()
            logger.log([(i,'007.4 Export group by results to Arrow',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

            # Read from a 10,000,000 row Pandas dataframe (from 0.45 seconds in 0.2.7 to 0.008 seconds in 0.10)
            start_time = time.perf_counter()
            arrow_df_summary = con.execute("select sum(v3) as v3 from arrow_df").fetch_arrow_table()
            print(arrow_df_summary)
            end_time = time.perf_counter()
            logger.log([(i,'007.5 Scan and aggregate over Arrow df',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # Load data for join queries (10.4 seconds to 3.4 seconds)
        x_csv_file = str(Path(venv_location).parent) + '/_data/J1_1e7_NA_0_0.csv'
        small_csv_file = str(Path(venv_location).parent) + '/_data/J1_1e7_1e1_0_0.csv'
        medium_csv_file = str(Path(venv_location).parent) + '/_data/J1_1e7_1e4_0_0.csv'
        big_csv_file = str(Path(venv_location).parent) + '/_data/J1_1e7_1e7_0_0.csv'
        table_suffix = '_csv'
        if duckdb_version in versions_without_enums:
            table_suffix = ''
        create_table_queries_joins = [
            f"DROP TABLE IF EXISTS x{table_suffix}",
            f"CREATE TABLE x{table_suffix} AS SELECT * FROM read_csv_auto('{x_csv_file}')",
            f"CREATE TABLE small{table_suffix} AS SELECT * FROM read_csv_auto('{small_csv_file}')",
            f"CREATE TABLE medium{table_suffix} AS SELECT * FROM read_csv_auto('{medium_csv_file}')",
            f"CREATE TABLE big{table_suffix} AS SELECT * FROM read_csv_auto('{big_csv_file}')",
            "CHECKPOINT"
        ]

        print('Beginning create_table_queries_joins', duckdb_version)
        start_time = time.perf_counter()
        for query in create_table_queries_joins:
            con.execute(query).fetchall()
        end_time = time.perf_counter()
        logger.log([(i,'008 Create tables from csvs joins',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        if duckdb_version not in versions_without_enums:
            id4_enum_statement = "SELECT id4 FROM x_csv UNION ALL SELECT id4 FROM small_csv UNION ALL SELECT id4 from medium_csv UNION ALL SELECT id4 from big_csv"
            id5_enum_statement = "SELECT id5 FROM x_csv UNION ALL SELECT id5 from medium_csv UNION ALL SELECT id5 from big_csv"
            convert_to_enum_queries_joins = [
                f"CREATE TYPE id4ENUM AS ENUM ({id4_enum_statement})",
                f"CREATE TYPE id5ENUM AS ENUM ({id5_enum_statement})",

                "CREATE TABLE small(id1 INT64, id4 id4ENUM, v2 DOUBLE)",
                "INSERT INTO small (SELECT * from small_csv)",

                "CREATE TABLE medium(id1 INT64, id2 INT64, id4 id4ENUM, id5 id5ENUM, v2 DOUBLE)",
                "INSERT INTO medium (SELECT * FROM medium_csv)",

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
            start_time = time.perf_counter()
            for query in convert_to_enum_queries_joins:
                con.execute(query).fetchall()
            end_time = time.perf_counter()
            logger.log([(i,'009 Convert to Enums for joins',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        join_queries = [
            "CREATE TABLE ans1 AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)",
            "CREATE TABLE ans2 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)",
            "CREATE TABLE ans3 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)",
            "CREATE TABLE ans4 AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)",
            "CREATE TABLE ans5 AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)",
            "CHECKPOINT",
        ]

        # Join queries from 28.5 seconds to 4.1 seconds
        print('Beginning join queries', duckdb_version)
        start_time = time.perf_counter()
        for query in join_queries:
            con.execute(query).fetchall()
        end_time = time.perf_counter()
        logger.log([(i,'010 Join queries',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # Export join results to Pandas from 47 seconds to 10 seconds
        start_time = time.perf_counter()
        for r in range(1, 6):
            result_table = "ans"+str(r)
            pandas_df = con.execute(f"select * from {result_table}").fetch_df()
            print(result_table, len(pandas_df), "rows")
        end_time = time.perf_counter()
        logger.log([(i,'011 Export join results to Pandas',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        # Skip pyarrow tests on version 0.2.7 since numpy wouldn't compile correctly
        if not duckdb_version == '0.2.7': 
             # Export join results to Arrow from  seconds to  seconds
            start_time = time.perf_counter()
            for r in range(1, 6):
                result_table = "ans"+str(r)
                arrow_df = con.execute(f"select * from {result_table}").fetch_arrow_table()
            end_time = time.perf_counter()
            logger.log([(i,'012 Export join results to Arrow',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])

        
        

    except Exception as err:
        import traceback
        print("ERROR in duckdb_version",duckdb_version)
        print(err)
        print(traceback.print_exc())
    finally:
        con.close()

# TODO: Apache Arrow read/write?
# TODO: S3 Parquet reader / writer? 

