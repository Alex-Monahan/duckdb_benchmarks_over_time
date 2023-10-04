

def run_duckdb_example():
    duckdb_location = r'/Users/alex/Documents/DuckDB/cli_0_8_1/duckdb'
    result = subprocess.run([duckdb_location, "-c","""SELECT 42;"""], capture_output=True, text=True)

    # print(result)
    print(result.stdout)

def run_subprocess_example():
    result = subprocess.run(['ls'], capture_output=True, text=True)

    # print(result)
    # print(result.stdout)

if __name__ == '__main__':
    import subprocess
    import timeit
    function_name = 'run_duckdb_example'
    repeat = 5
    number = 1

    print(function_name,':',timeit.repeat(f'{function_name}()', setup=f'from __main__ import {function_name}',repeat=repeat, number=number))

    function_name = 'run_subprocess_example'
    print(function_name,':',timeit.repeat(f'{function_name}()', setup=f'from __main__ import {function_name}',repeat=repeat, number=number))

