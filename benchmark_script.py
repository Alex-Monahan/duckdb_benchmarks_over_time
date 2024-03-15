import pandas
import duckdb
import json
import os 
import time
import shutil
import sys
from pathlib import Path

from SQLiteLogger import SQLiteLogger

# At what level do I want parameters? Do I want to pass them all the way in from the top level script?
# The advantage there is that I could do something different based on the version. 
# But I can also look up the version in here.

# Let's start with all the parameters in here.

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

# TODO: use a persistent database
# TODO: Delete that persistent database
# TODO: Set a temp directory

repeat = 5

for i in range(repeat):
    print(sys.executable)
    python_executable_location = sys.executable
    
    start_time = time.perf_counter()
    con = duckdb.connect(':memory:')
    duckdb_version = con.execute('select version()').fetchall()[0][0]
    print(duckdb_version)

    my_df = pandas.DataFrame.from_dict({'a': [42]})
    pandas_results = con.execute("select * from my_df").df()
    print(pandas_results)
    end_time = time.perf_counter()
    print('Time in repeat',str(i),(end_time - start_time),'seconds')
    logger.log([(i,'Fetch version, query pandas',json.dumps({'duckdb_version':duckdb_version}),(end_time - start_time))])
