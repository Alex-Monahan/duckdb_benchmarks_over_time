import sqlite3
import os

sqlite_filename = 'sqlite_test.db'
# try:
#     os.remove(sqlite_filename)
# except OSError:
#     pass

con = sqlite3.connect(sqlite_filename)
cur = con.cursor()

def create_results_table(cur):
    cur.execute("""
        create table if not exists results (
            run_id int,
            benchmark varchar,
            scenario varchar
        )            
    """)
def get_new_run_id(cur):
    max_run_id = cur.execute("""select max(run_id) as max_run_id from results""").fetchall()[0][0]

    print(max_run_id)
    if max_run_id is None:
        run_id = 1
    else:
        run_id = max_run_id + 1
    
    return run_id

create_results_table(cur)
run_id = get_new_run_id(cur)

data = [
    (run_id, 'tpch', 'first_test'),
    (run_id, 'tpch', 'second_test')
]
cur.executemany("""insert into results values(?, ?, ?)""", data)
con.commit()

results = cur.execute("""select * from results""").fetchall()

print(results)