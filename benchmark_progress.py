import time
from threading import Thread 
from datetime import datetime, timedelta

from SQLiteLogger import SQLiteLogger


def log_on_regular_cadence(total_time, interval):
    logger = SQLiteLogger('benchmark_log_python.db', delete_file=False)
    global stop_logging
    stop_logging = False
    start_time_counter = time.perf_counter()
    end_time = time.time() + total_time
    while time.time() < end_time:
        logger.pprint(logger.get_results())
        print(datetime.now().isoformat(sep=' '))
        print('Elapsed time:',timedelta(seconds=time.perf_counter() - start_time_counter))
        if stop_logging:
            break
        time.sleep(interval)

t = Thread(target=log_on_regular_cadence,args=(5,1,))
t.start()
print('Thread started')
time.sleep(3)
stop_logging=True
t.join()
# log_on_regular_cadence(5,1)