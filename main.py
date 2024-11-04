"""
Main module
"""

import datetime as dt
import json
import os
from time import sleep
import pyodbc
import func
import query


def main():
    try:
        # Startup
        cwd = os.getcwd()
        if 'HOME' in os.environ:
            root = os.environ['HOME']
        else:
            root = os.environ['HOMEDRIVE'] + os.environ['HOMEPATH']
        rootop1 = os.path.join(root, '.eq_vars')
        rootop2 = 'C:\\.eq_vars'
        if os.path.exists(rootop1):
            root = rootop1
        else:
            root = rootop2
        with open(os.path.join(root, 'connect.json')) as file:
            connop = json.load(file)
        with open(os.path.join(root, 'qvars.json')) as file:
            qvars = json.load(file)
        if not (func.validate_keys(connop, ['sqlserver'])
                and func.validate_keys(connop['sqlserver'],
                                  ['driver',
                                   'host',
                                   'database',
                                   'user',
                                   'password'])):
            raise KeyError('JSON files malformed; refer to README.')
        outdir = os.path.join(cwd, 'out')
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        logfile = os.path.join(cwd, 'log.txt')
        if not os.path.exists(logfile):
            with open(logfile, 'w'):
                pass
        cyclefile = os.path.join(cwd, 'last.dat')
        if not os.path.exists(cyclefile):
            with open(cyclefile, 'w') as f:
                pass

        # Main loop
        while True:
            next_start = dt.datetime.now().astimezone() + dt.timedelta(hours=1)
            with open(cyclefile) as f:
                last = f.read()
            if len(last) == 0 or func.convert_dt_str(last).date() < func.get_now().date():
                func.clear()
                with pyodbc.connect(driver=connop['sqlserver']['driver'],
                                    server=connop['sqlserver']['host'],
                                    database=connop['sqlserver']['database'],
                                    uid=connop['sqlserver']['user'],
                                    pwd=connop['sqlserver']['password']) as conn:
                    func.plog(logfile, f"Connected at {func.get_now_str()} to SQL Server database, driver version: {conn.getinfo(pyodbc.SQL_DRIVER_VER)}")
                    with conn.cursor() as cur:
                        for k, v in query.QUERIES.items():
                            outfile = os.path.join(outdir, f"{k}_{func.get_now().strftime('%Y%m%d')}.csv")
                            func.plog(logfile, f"Started running query `{k}` at {func.get_now_str()}")
                            cur.execute(v, qvars[k]["args"])
                            func.plog(logfile, f"Query `{k}` returned successfully at {func.get_now_str()}")
                            func.query_to_csv(outfile, cur, header=qvars[k]["header"])
                            func.plog(logfile, f"Wrote results to `{outfile}` at {func.get_now_str()}")
                with open(cyclefile, 'w') as f:
                    f.write(func.get_now_str())
            sleep_interval = (next_start - dt.datetime.now().astimezone()).seconds
            print(f'Next cycle will begin at {next_start}')
            sleep(sleep_interval)
    except KeyboardInterrupt:
        func.plog(logfile, f"Script terminated via keyboard interrupt at {func.get_now_str()}")
    except (KeyError, OSError, json.JSONDecodeError) as e:
        print(repr(e))

if __name__ == "__main__":
    main()
