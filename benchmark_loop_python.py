# Subprocess to create new virtual environment
# pip install DuckDB of the right version
#   Depends on ARM MacOS binary and the correct version of Python
# Run a query from within Python
# Install the right version of Pandas for each DuckDB version
# Example Pandas dataframe (H2O.ai?)

from datetime import datetime
import subprocess
import shutil
import duckdb

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

    commands = [
        name+'/bin/pip3',
        'install',
        'duckdb=='+duckdb_version,
        ' '.join(libraries_list),
    ]
    print(' '.join(commands))
    result = subprocess.run(commands, capture_output=True, text=True)
    print(result.stdout)

    commands = [
        name+'/bin/python3.9',
        '-c',
        """
import pandas
import duckdb
con = duckdb.connect(':memory:')
results = con.execute('select version()').fetchall()
print(results)

my_df = pandas.DataFrame.from_dict({'a': [42]})
pandas_results = con.execute("select * from my_df").df()
print(pandas_results)

"""
    ]
    result = subprocess.run(commands, capture_output=True, text=True)
    print(result.stdout)
    print('result.stderr:\n', result.stderr)


# Deduce the correct pandas version
con = duckdb.connect()
pandas_versions = con.execute("from 'pandas_versions.csv'").df()
print(pandas_versions)

i = 0
for version, details in versions.items():
    latest_pandas_version = con.execute(f"""
        from pandas_versions 
        select 
            max_by(version,max_upload_time) as max_version
        where
            max_upload_time <= '{details['date']}'::datetime
            and version not ilike '%rc%'
        """).fetchall()[0][0]
    print('DuckDB version:',version,'Pandas version:',latest_pandas_version)
    create_virtualenv('./venv_', version, ['pandas=='+latest_pandas_version])


