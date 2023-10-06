from urllib.request import urlretrieve
import shutil
import os
import stat

def download_cli_versions(version_list):
    # https://github.com/duckdb/duckdb/releases/download/v0.9.0/duckdb_cli-osx-universal.zip
    url_core = r'https://github.com/duckdb/duckdb/releases/download/v'
    environment = r'osx-universal.zip'

    for version in version_list:
        url = url_core + version + r'/duckdb_cli-' + environment
        filename = 'duckdb_' + version.replace('.','_') + '.zip'
        unzipped_filename = filename.replace('.zip','')
        if os.path.exists(unzipped_filename):
            continue

        path, headers = urlretrieve(url, filename)
        
        # unzip
        shutil.unpack_archive(filename)

        # rename to version specific name and grant execute permissions
        os.rename('duckdb', unzipped_filename)
        os.chmod(unzipped_filename, mode=stat.S_IEXEC)

def run_duckdb_example(duckdb_location):
    result = subprocess.run([duckdb_location, "-c","""SELECT 42;"""], capture_output=True, text=True)
    # print(result)
    print(result.stdout)

def run_subprocess_example():
    result = subprocess.run(['ls'], capture_output=True, text=True)

    # print(result)
    # print(result.stdout)

# def generate_tpch(duckdb_location, ):
#     result = subprocess.run([duckdb_location, "-c","""SELECT 42;"""], capture_output=True, text=True)
#     # print(result)
#     print(result.stdout)


if __name__ == '__main__':
    import subprocess
    import timeit
    from datetime import datetime
    import platform

    # MacOS with arm64 support versions:
    versions = {
        # No precompiled CLI prior to 0.1.9
        # '0.1.3': {'date':datetime.strptime('2020-02-03','%Y-%m-%d')},
        # '0.1.5': {'date':datetime.strptime('2020-03-02','%Y-%m-%d')},
        # '0.1.6': {'date':datetime.strptime('2020-04-05','%Y-%m-%d')},
        # '0.1.7': {'date':datetime.strptime('2020-05-04','%Y-%m-%d')},
        # '0.1.8': {'date':datetime.strptime('2020-05-29','%Y-%m-%d')},
        '0.1.9': {'date':datetime.strptime('2020-06-19','%Y-%m-%d')},
        '0.2.0': {'date':datetime.strptime('2020-07-23','%Y-%m-%d')},
        '0.2.1': {'date':datetime.strptime('2020-08-29','%Y-%m-%d')},
        '0.2.2': {'date':datetime.strptime('2020-11-01','%Y-%m-%d')},
        '0.2.3': {'date':datetime.strptime('2020-12-03','%Y-%m-%d')},
        '0.2.4': {'date':datetime.strptime('2021-02-01','%Y-%m-%d')},
        '0.2.5': {'date':datetime.strptime('2021-03-10','%Y-%m-%d')},
        '0.2.6': {'date':datetime.strptime('2021-05-08','%Y-%m-%d')},
        '0.2.7': {'date':datetime.strptime('2021-06-14','%Y-%m-%d')},
        '0.2.8': {'date':datetime.strptime('2021-08-02','%Y-%m-%d')},
        '0.2.9': {'date':datetime.strptime('2021-09-06','%Y-%m-%d')},
        '0.3.0': {'date':datetime.strptime('2021-10-06','%Y-%m-%d')},
        '0.3.1': {'date':datetime.strptime('2021-11-16','%Y-%m-%d')},
        '0.3.2': {'date':datetime.strptime('2022-02-07','%Y-%m-%d')},
        '0.3.3': {'date':datetime.strptime('2022-04-11','%Y-%m-%d')},
        '0.3.4': {'date':datetime.strptime('2022-04-25','%Y-%m-%d'),'osx-universal':True},
        '0.4.0': {'date':datetime.strptime('2022-06-20','%Y-%m-%d'),'osx-universal':True},
        '0.5.1': {'date':datetime.strptime('2022-09-19','%Y-%m-%d'),'osx-universal':True},
        '0.6.1': {'date':datetime.strptime('2022-12-06','%Y-%m-%d'),'osx-universal':True},
        '0.7.1': {'date':datetime.strptime('2023-02-27','%Y-%m-%d'),'osx-universal':True},
        '0.8.1': {'date':datetime.strptime('2022-06-13','%Y-%m-%d'),'osx-universal':True},
        '0.9.0': {'date':datetime.strptime('2022-09-26','%Y-%m-%d'),'osx-universal':True}
    }

    # Linux CLI versions
    # versions = ['0.2.9', '0.3.0', '0.3.1', '0.3.2', '0.3.3', '0.3.4', '0.4.0', '0.5.1', '0.6.1', '0.7.1', '0.8.1', '0.9.0']

    my_os = platform.platform()
    my_processor_type = platform.processor()

    version_list = []
    # Filter the versions down if running on macos
    for version, version_details in versions.items():
        if 'macOS' in my_os and 'arm' in my_processor_type:
            if version_details.get('osx-universal', False):
                version_list.append(version)
        else:
            version_list.append(version)
    
    print(version_list)

    download_cli_versions(version_list)
    cli_filenames = []
    for version in version_list:
        cli_filenames.append('duckdb_' + version.replace('.','_'))

    print(cli_filenames)

    function_name = 'run_duckdb_example'
    repeat = 5
    number = 1

    for filename in cli_filenames:
        print(function_name,':',timeit.repeat(f'{function_name}(".//{filename}")', setup=f'from __main__ import {function_name}',repeat=repeat, number=number))

    function_name = 'run_subprocess_example'
    print(function_name,':',timeit.repeat(f'{function_name}()', setup=f'from __main__ import {function_name}',repeat=repeat, number=number))

