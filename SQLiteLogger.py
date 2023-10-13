import sqlite3
import os

class SQLiteLogger():
    def __init__(self, sqlite_filename, delete_file=False):
        self.filename = sqlite_filename
        if delete_file:
            try:
                os.remove(sqlite_filename)
            except OSError:
                pass
        
        self.con = sqlite3.connect(sqlite_filename)
        self.cur = self.con.cursor()
        self.create_results_table()
        self.run_id = self.get_new_run_id()

    def create_results_table(self):
        self.cur.execute("""
            create table if not exists results (
                run_id int,
                repeat_id int,
                benchmark varchar,
                scenario varchar,
                time float 
            )            
        """)
    def get_new_run_id(self):
        max_run_id = self.cur.execute("""select max(run_id) as max_run_id from results""").fetchall()[0][0]

        print(max_run_id)
        if max_run_id is None:
            run_id = 1
        else:
            run_id = max_run_id + 1
        
        return run_id

    def log(self, input_data):
        # Add in the run_id to each row
        data = []
        for row in input_data:
            data.append((self.run_id,) + row)

        self.cur.executemany("""insert into results values(?, ?, ?, ?, ?)""", data)
        self.con.commit()
    
    def get_results(self):
        return self.cur.execute("""select * from results""").fetchall()
    
    def pprint(self, results):
        print_string = '[' + '\n'
        for row in results:
            print_string += '\t' + str(row) + '\n'
        print_string += ']' + '\n'
        print(print_string)


if __name__ == '__main__':
    logger = SQLiteLogger(sqlite_filename='sqlite_test.db', delete_file=False)

    data = [
        ('tpch', 1, '[0.01]', .123),
        ('tpch', 1, '[0.1]', .1234)
    ]

    logger.log(data)

    results = logger.get_results()

    logger.pprint(results)