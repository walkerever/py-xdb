#!/usr/bin/env python3
# Yonghang Wang

import argparse
import os
import sys
import string
import random
import pandas
import re
from xtable import xtable
from sqlalchemy import create_engine
import traceback
import sqlite3
import json


def xdb_main():
    parser = argparse.ArgumentParser(description="generic SQL client. Yonghang Wang, wyhang@gmail.com, 2021")
    parser.add_argument( "-d", "--db", "--database","--engine",dest="db", default=":memory:",  help="database name. default sqlite in memory. use alias in cfg file or full sqlalchedmy url for other dbms.")
    parser.add_argument( "-q", "--sql", "--query",dest="sql", default=None,  help="SQL stmt or file containing sql query")
    parser.add_argument( "-C", "--configfile", dest="cfgfile", default="~/.dbx.dbs.json",  help="config file to store database details.")
    parser.add_argument( "-B", "--sqldelimiter",dest="sqlsep", default='@@',  help="sql delimiter in SQL files")
    parser.add_argument( "-X", "--debug", dest="debug", action="store_true", default=False, help="debug mode",)
    parser.add_argument( "--encoding",dest="encoding", default="utf-8",  help="default encoding")
    parser.add_argument( "--json", dest="json", action="store_true", default=False, help="dump result in JSON",)
    parser.add_argument( "--yaml", dest="yaml", action="store_true", default=False, help="dump result in YAML",)
    parser.add_argument( "--csv", dest="csv", action="store_true", default=False, help="dump result in CSV",)
    parser.add_argument( "--html", dest="html", action="store_true", default=False, help="dump result in HTML",)
    parser.add_argument( "--markdown", dest="markdown", action="store_true", default=False, help="dump result in Markdown",)
    parser.add_argument( "--pivot", dest="pivot", action="store_true", default=False, help="pivot the result. better for wide table.",)
    args = parser.parse_args()
    
    def _x(s,debug=args.debug) :
        if debug :
            for ln in s.splitlines() :
                print("# "+ln,file=sys.stderr,flush=True)

    def rand_name(n) :
        m = max(n,3)
        return "_ix_" + "".join([random.choice(string.ascii_lowercase) for _ in range(m)])

    dbs = {}
    if os.path.isfile(os.path.expanduser(args.cfgfile)) :
        with open(os.path.expanduser(args.cfgfile),"r") as f :
            for r in json.loads(f.read()) :
                if "alias" in r and "URL" in r :
                    dbs[r["alias"]] = r["URL"]

    if dbs and args.db in dbs  :
        args.db = dbs[args.db]

    if "//" in args.db :
        try :
            engine = create_engine(args.db)
            con = engine.connect()
        except :
            print(traceback.format_exc(),file=sys.stderr,flush=True)
            sys.exit(-1)
    else :
        try :
            con = sqlite3.connect(args.db)
        except :
            print(traceback.format_exc(),file=sys.stderr,flush=True)
            sys.exit(-1)

    sqlstmt = args.sql
    if sqlstmt :
        if os.path.isfile(sqlstmt) :
            _x("loading query from {}".format(sqlstmt))
            with open(sqlstmt,"r") as f :
                processed_sql = ""
                for ln in f.readlines() :
                    if re.search(r"^\s*\-\-",ln):
                        continue
                    processed_sql += ln
                sqlstmt = processed_sql 

    sqlstmt = sqlstmt or ""
    for sql in sqlstmt.split(args.sqlsep) :
        if not sql :
            continue
        _x("{}".format(sql))
        xt = None
        try :
            results = con.execute(sql)
            rows = results.rowcount
            header = [k for k in results.keys()]
            if header :
                data = [r for r in results]
                xt = xtable(data=data, header=header) 
                print("# {} rows selected.".format(rows), file=sys.stderr, flush=True)
            else :
                print("# {} rows affected.".format(rows), file=sys.stderr, flush=True)
        except :
            print(traceback.format_exc(),file=sys.stderr,flush=True)
            con.close()
            sys.exit(-1)

        con.close()

        if not xt :
            continue
        if args.json :
            print(xt.json())
        elif args.yaml :
            print(xt.yaml())
        elif args.csv :
            print(xt.csv())
        elif args.html :
            print(xt.html())
        elif args.markdown:
            print(xt.markdown())
        else :
            print(xt)

if __name__ == "__main__":
    xdb_main()
