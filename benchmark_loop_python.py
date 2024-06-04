# Subprocess to create new virtual environment
# pip install DuckDB of the right version
#   Depends on ARM MacOS binary and the correct version of Python
# Run a query from within Python
# Install the right version of Pandas for each DuckDB version
# Example Pandas dataframe (H2O.ai?)

from datetime import datetime, timedelta
import time
import subprocess
import shutil
import duckdb
from threading import Thread
import os
from contextlib import redirect_stdout

from SQLiteLogger import SQLiteLogger

# Versions:
# 0.2.7 is the first with MacOS ARM
# Runs on Python 3.9
# (0.10.0 also runs on Python 3.9)

versions = {
    # No precompiled CLI prior to 0.1.9
    # '0.1.3': {'date':datetime.strptime('2020-02-03','%Y-%m-%d')},
    # '0.1.5': {'date':datetime.strptime('2020-03-02','%Y-%m-%d')},
    # '0.1.6': {'date':datetime.strptime('2020-04-05','%Y-%m-%d')},
    # '0.1.7': {'date':datetime.strptime('2020-05-04','%Y-%m-%d')},
    # '0.1.8': {'date':datetime.strptime('2020-05-29','%Y-%m-%d')},
    # '0.1.9': {'date':datetime.strptime('2020-06-19','%Y-%m-%d')},
    # '0.2.0': {'date':datetime.strptime('2020-07-23','%Y-%m-%d')},
    # '0.2.1': {'date':datetime.strptime('2020-08-29','%Y-%m-%d')},
    # '0.2.2': {'date':datetime.strptime('2020-11-01','%Y-%m-%d')},
    # '0.2.3': {'date':datetime.strptime('2020-12-03','%Y-%m-%d')},
    # '0.2.4': {'date':datetime.strptime('2021-02-01','%Y-%m-%d')},
    # '0.2.5': {'date':datetime.strptime('2021-03-10','%Y-%m-%d')},
    # '0.2.6': {'date':datetime.strptime('2021-05-08','%Y-%m-%d')},

    # 0.2.7 is the first with MacOS ARM
    '0.2.7': {'date':datetime.strptime('2021-06-14','%Y-%m-%d')},
    '0.2.8': {'date':datetime.strptime('2021-08-02','%Y-%m-%d')},
    '0.2.9': {'date':datetime.strptime('2021-09-06','%Y-%m-%d')},
    '0.3.0': {'date':datetime.strptime('2021-10-06','%Y-%m-%d')},
    '0.3.1': {'date':datetime.strptime('2021-11-16','%Y-%m-%d')},
    '0.3.2': {'date':datetime.strptime('2022-02-07','%Y-%m-%d')},
    # 0.3.3 did not upload to pip correctly so it should be skipped
    # '0.3.3': {'date':datetime.strptime('2022-04-11','%Y-%m-%d')},
    '0.3.4': {'date':datetime.strptime('2022-04-25','%Y-%m-%d'),'osx-universal':True},
    '0.4.0': {'date':datetime.strptime('2022-06-20','%Y-%m-%d'),'osx-universal':True},
    '0.5.1': {'date':datetime.strptime('2022-09-19','%Y-%m-%d'),'osx-universal':True},
    '0.6.1': {'date':datetime.strptime('2022-12-06','%Y-%m-%d'),'osx-universal':True},
    '0.7.1': {'date':datetime.strptime('2023-02-27','%Y-%m-%d'),'osx-universal':True},
    '0.8.1': {'date':datetime.strptime('2023-06-13','%Y-%m-%d'),'osx-universal':True},
    '0.9.0': {'date':datetime.strptime('2023-09-26','%Y-%m-%d'),'osx-universal':True},
    '0.9.1': {'date':datetime.strptime('2023-10-11','%Y-%m-%d'),'osx-universal':True},
    '0.9.2': {'date':datetime.strptime('2023-11-14','%Y-%m-%d'),'osx-universal':True},
    '0.10.0': {'date':datetime.strptime('2024-02-13','%Y-%m-%d'),'osx-universal':True},
    '0.10.1': {'date':datetime.strptime('2024-03-18','%Y-%m-%d'),'osx-universal':True},
    '0.10.2': {'date':datetime.strptime('2024-04-17','%Y-%m-%d'),'osx-universal':True},
}

# First, install Python 3.9 if it isn't installed already
# brew install python@3.9
# Then, use it to create a virtual environment
# Then, point to the right pip3 and install packages
# Then, point to the right python and run a script with it
# Then make a loop to do this for multiple
def create_virtualenv(prefix, duckdb_version, libraries_list):
    name = prefix + duckdb_version.replace('.','_')
    try:
        shutil.rmtree(name)
        print('Deleted virtual environment folder',name)
    except:
        print('Failed to delete virtual environment folder',name)

    commands = [
        'python3.9',
        '-m',
        'venv',
        name,
    ]
    result = subprocess.run(commands, capture_output=True, text=True)
    print(result.stdout)
    print('Created virtual environment',name)

    with open(f'{name}/bin/requirements.txt', 'w') as f:
        f.writelines([
            '\n'.join(libraries_list)+'\n',
            'duckdb=='+version,
        ])

    commands = [
        name+'/bin/pip3',
        'install', 
        '-r',
        name+'/bin/requirements.txt'
    ]

    print('|'+' '.join(commands)+'|')
    result = subprocess.run(commands, capture_output=True, text=True)
    print(result.stdout)
    print('result.stderr:\n', result.stderr)

    print('virtual environment created and libraries added')

def run_python_script(prefix, duckdb_version, script_filename):
    name = prefix + duckdb_version.replace('.','_')
    with open(script_filename, 'r') as script_file:
        python_script = script_file.read()

    commands = [
        name+'/bin/python3.9',
        '-c',
        python_script,
    ]
    # print(' '.join(commands))
    result = subprocess.run(commands, capture_output=True, text=True)
    print(result.stdout)
    print('result.stderr:\n', result.stderr)


def log_on_regular_cadence(total_time, interval):
    logger = SQLiteLogger('benchmark_log_python.db', delete_file=False)
    global stop_logging
    stop_logging = False
    start_time_counter = time.perf_counter()
    end_time = time.time() + total_time
    while time.time() < end_time:
        logger.pprint(logger.get_results())
        print(datetime.now().isoformat(sep=' '),flush=True)
        print('Elapsed time:',timedelta(seconds=time.perf_counter() - start_time_counter),flush=True)
        if stop_logging:
            break
        time.sleep(interval)


log_path = './logs/'
if not os.path.exists(log_path):
    os.makedirs(log_path)
filename = log_path + 'log.txt'
runtime = datetime.now().isoformat(sep=' ')
try:
    shutil.move(filename,filename.replace('log.txt',f'archived_at_{runtime}_log.txt'))
    os.remove(filename)
except:
    pass

shutil.move('benchmark_log_python.db','benchmark_log_python.db'.replace('.db',f'archived_at_{runtime}.db'))

with open(filename, 'a') as f:
    with redirect_stdout(f):
        print('Starting run',flush=True)
        # Deduce the correct pandas version
        con = duckdb.connect()
        pandas_versions = con.execute("from 'pandas_versions.csv'").df()
        # print(pandas_versions)
        pyarrow_versions = con.execute("from 'pyarrow_versions.csv'").df()
        # print(pyarrow_versions)

        logger = SQLiteLogger('benchmark_log_python.db', delete_file=True)


        # SELECT  
        #   name,
        #   version,
        #   max(upload_time) as max_upload_time
        # FROM `bigquery-public-data.pypi.distribution_metadata` 
        # WHERE 
        #   name = 'pyarrow'
        #   and version not like '%.post%'
        # group by
        #   name,
        #   version
        # order by 
        #   max_upload_time desc

        # TODO: REMOVE. Filter down the versions for testing
        create_environments = False
        run_scripts = True
        # versions_to_test = ['0.2.7']
        # versions_to_test = ['0.3.1']
        # versions_to_test = ['0.2.7', '0.2.8', '0.2.9', '0.3.0', '0.3.1', '0.3.2', '0.3.4', '0.4.0', '0.5.1', '0.6.1', '0.7.1']
        # versions_to_test = ['0.2.7', '0.7.1', '0.8.1', '0.10.2']
        # versions_to_test = ['0.2.8','0.2.9','0.3.0']
        versions_to_test = ['0.2.7', '0.2.8', '0.2.9', '0.3.0',] # '0.3.1', '0.3.2', ] # '0.3.4', '0.4.0', '0.5.1', '0.6.1', '0.7.1',] # '0.8.1', '0.9.0',] # '0.9.1', '0.9.2',] # '0.10.0', '0.10.1', '0.10.2']
        versions = {k: versions.get(k) for k in versions_to_test}

        # t = Thread(target=log_on_regular_cadence,args=(1000000,300,))
        t = Thread(target=log_on_regular_cadence,args=(1000000,10,))
        t.start()
        print('Background logging thread started',flush=True)

        for version, details in versions.items().__reversed__():
        # for version, details in versions.items():
            latest_pandas_version = con.execute(f"""
                from pandas_versions 
                select 
                    max_by(version,max_upload_time) as max_version
                where
                    max_upload_time <= '{details['date']}'::datetime
                    and version not ilike '%rc%'
                """).fetchall()[0][0]

            latest_pyarrow_version = con.execute(f"""
                from pyarrow_versions 
                select 
                    max_by(version,max_upload_time) as max_version
                where
                    max_upload_time <= '{details['date']}'::datetime
                """).fetchall()[0][0]
            
            # # Pyarrow 5.0.0 is broken, and 4.0.1 does not compile numpy
            # if latest_pyarrow_version == '5.0.0':
            #     latest_pyarrow_version = '4.0.1'
            # print('DuckDB version:',version,'Pandas version:',latest_pandas_version,'Pyarrow version:',latest_pyarrow_version)
            # create_virtualenv('./venv_', version, ['pyarrow=='+latest_pyarrow_version])

            if create_environments:
                if version in ['0.2.7','0.2.8','0.2.9','0.3.0']:
                    # Then pyarrow installation does not work (numpy failed to compile from source), so skip it
                    create_virtualenv('./venv_', version, ['pandas=='+latest_pandas_version])
                else:
                    create_virtualenv('./venv_', version, ['pandas=='+latest_pandas_version, 'pyarrow=='+latest_pyarrow_version])
            
            if run_scripts:
                start_time = time.perf_counter()
                run_python_script('./venv_', version,'./benchmark_script.py')

                logger.pprint(logger.get_results())
                end_time = time.perf_counter()
                print(f'Running script for version {version} took {round(end_time-start_time,1)} seconds',flush=True)

        stop_logging=True
        t.join()

