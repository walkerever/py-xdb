#!/usr/bin/env python3
# Yonghang Wang

import argparse
import os
import sys
import string
import random
import re
from xtable import xtable
import traceback
import sqlite3
import json
from collections import deque
from sqlalchemy import create_engine, text as sqltext


def xdb_main():
    parser = argparse.ArgumentParser(description="generic SQL client. Yonghang Wang, wyhang@gmail.com, 2021")
    parser.add_argument( "-d", "--db", "--database","--engine",dest="db", default=":memory:",  help="database name. default sqlite in memory. use alias in cfg file or full sqlalchedmy url for other dbms.")
    parser.add_argument( "-t", "--table", dest="tables", action="append", default=[],  help="specify CSV files to load as tables.")
    parser.add_argument( "-q", "--sql", "--query",dest="sql", default=None,  help="SQL stmt or file containing sql query. if SQL file, only run the last SQL statement.")
    parser.add_argument( "-B", "--sqldelimiter",dest="sqlsep", default=';',  help="sql delimiter in SQL files")
    parser.add_argument( "--all",dest="all", action="store_true", default=False,  help="run all SQL in SQL file.")
    parser.add_argument( "--noheader",dest="noheader", action="store_true", default=False,  help="indicate the CSV file(s) have no header")
    parser.add_argument( "-X", "--debug", dest="debug", action="store_true", default=False, help="debug mode",)
    parser.add_argument( "--encoding",dest="encoding", default="utf-8",  help="default encoding")
    parser.add_argument( "--json", dest="json", action="store_true", default=False, help="dump result in JSON",)
    parser.add_argument( "--yaml", dest="yaml", action="store_true", default=False, help="dump result in YAML",)
    parser.add_argument( "--csv", dest="csv", action="store_true", default=False, help="dump result in CSV",)
    parser.add_argument( "--html", dest="html", action="store_true", default=False, help="dump result in HTML",)
    parser.add_argument( "--markdown", dest="markdown", action="store_true", default=False, help="dump result in Markdown",)
    parser.add_argument( "--pivot", dest="pivot", action="store_true", default=False, help="pivot the result. better for wide table.",)
    parser.add_argument( "--wrap", dest="wrap", action="store_true", default=False, help="wrap the result. better for wide table.",)
    parser.add_argument( "-C", "--configfile", dest="cfgfile", default="~/.xdb.dbs.json",  help="config file to store database details.")
    args = parser.parse_args()
    
    example_cfg = """
{ "databases": [ { "alias": "xmini-sample", "URL": "postgresql+psycopg2://postgres:XXXXXX@localhost:5432/sample" }, { "alias": "home-sample", "URL": "postgresql+psycopg2://postgres:XXXXXXX@home.XXXXXXX.com:5432/sample" } ], "plugins": { "\\d": "select table_schema, table_name, table_type from INFORMATION_SCHEMA.tables where lower(table_schema) not in ('information_schema','pg_catalog') order by 1,3 desc,2" } }

"""
    WRAP_OUTPUT = False
    PIVOT_OUTPUT = False
    PLUGINS = {}

    def _x(s,debug=args.debug) :
        if debug :
            for ln in s.splitlines() :
                print("# "+ln,file=sys.stderr,flush=True)
    def _x_sql(s,debug=args.debug) :
        if debug :
            try :
                from pygments import highlight
                from pygments.lexers.sql import SqlLexer
                from pygments.formatters import Terminal256Formatter
                s = highlight(s,SqlLexer(),Terminal256Formatter())
                for ln in s.splitlines() :
                    print("--   "+ln,file=sys.stderr,flush=True)
            except :
                _x(s,debug)

    def rand_name(n=8,prefix="/tmp/tmp_") :
        m = max(n,3)
        return prefix + "".join([random.choice(string.ascii_lowercase) for _ in range(m)])

    # refresh data if needed
    def refresh_tables(stmt_tables) :
        import pandas
        for tblstmt in stmt_tables :
            tblstmt = "="+tblstmt
            arr = tblstmt.split("=")
            csv = arr[-1]
            tbl = arr[-2] or csv.split(".")[0]
            tblmode="replace"
            if "+" in tbl :
                tbl = re.sub(r"\+$","",tbl)
                tblmode="append"
            _x("table    = {}".format(tbl))
            _x("csv      = {}".format(csv))
            _x("tblmode  = {}".format(tblmode))
            try :
                df = pandas.read_csv(os.path.expanduser(csv),encoding=args.encoding)
            except :
                tf = rand_name()
                with open(tf,"w",encoding=args.encoding) as fw :
                    with open(os.path.expanduser(csv),"r",encoding=args.encoding,errors="ignore") as fr :
                        fw.write(fr.read())
                df = pandas.read_csv(tf,encoding=args.encoding)
            df.to_sql(tbl,con,if_exists=tblmode,index=False)
            try :
                con.commit()
            except :
                pass

    def run_sql(sql) :
        nonlocal WRAP_OUTPUT
        nonlocal PIVOT_OUTPUT
        sqlstmt = sql
        if sqlstmt :
            if os.path.isfile(sqlstmt) :
                _x("loading query from {}".format(sqlstmt))
                with open(sqlstmt,"r") as f :
                    processed_sql = ""
                    for ln in f.readlines() :
                        ln = re.sub(r"\-\-.*$","",ln)
                        processed_sql += ln
                    if not args.all :
                        sqlstmt = [s for s in processed_sql.split(args.sqlsep) if re.search(r"\S+",s)][-1]
                    else :
                        sqlstmt = processed_sql 
    
        sqlstmt = sqlstmt or ""
        for sql in sqlstmt.split(args.sqlsep) :
            sql = sql.strip()
            sql = sql.strip(";")
            if not sql :
                continue
            _x_sql(sql)
            if PLUGINS and sql in PLUGINS :
                sql = PLUGINS[sql]
            #_x("{}".format(sql))
            xt = None
            try :
                results = con.execute(sqltext(sql))
                rows = results.rowcount
                header = [k for k in results.keys()]
                if header :
                    data = [r for r in results]
                    xt = xtable(data=data, header=header) 
                    _x("{} rows selected.".format(len(data)))
                else :
                    _x("{} rows affected.".format(rows))
            except :
                msg = []
                for ln in traceback.format_exc().splitlines() :
                    #if re.search(r"^\s+",ln) or re.search(r"^Traceback",ln) or re.search(r"Background on.*sqlalche.me",ln) :
                    #    continue
                    if ln :
                        msg.append("#  " + ln.rstrip())
                print("\n".join(msg),file=sys.stderr,flush=True)
                #con.close()
                #sys.exit(-1)
    
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
            elif args.pivot or PIVOT_OUTPUT:
                print(xt.pivot())
            elif args.wrap or WRAP_OUTPUT :
                print(xt.wrap())
            else :
                print(xt)

    def interactive() :
        nonlocal WRAP_OUTPUT
        nonlocal PIVOT_OUTPUT
        ptok = True
        try :
            from pygments.lexers.sql import SqlLexer
            from prompt_toolkit import PromptSession
            from prompt_toolkit.completion import WordCompleter
            from prompt_toolkit.lexers import PygmentsLexer
            _x_completer = None
            try :
                lst = []
                results = con.execute("select table_schema from information_schema.tables union select table_name from information_schema.tables ")
                for r in results :
                    lst.append(r[0])
                _x_completer = WordCompleter(lst,ignore_case=True)
            except :
                _x_completer = None
            _x_session = PromptSession(lexer=PygmentsLexer(SqlLexer),completer=_x_completer)
        except :
            ptok = False
        history = deque(maxlen=200)
        current_command = ""
        while True :
            if ptok :
                _x_sin = _x_session.prompt('[xdb] $ ')
            else :
                _x_sin = input('[xdb] $ ')
            if not current_command and re.search(r"^\s*\\set\s+wrap\s*$",_x_sin) :
                WRAP_OUTPUT=True
                current_command = ""
                continue
            if not current_command and re.search(r"^\s*\\set\s+pivot\s*$",_x_sin) :
                PIVOT_OUTPUT=True
                current_command = ""
                continue
            if _x_sin.startswith("\\") and not _x_sin.rstrip().endswith(";") :
                _x_sin += ";"
            if not current_command and re.search(r"^\s*\\r\s*\d+",_x_sin) :
                m = re.search(r"\\r\s*(\d+)",_x_sin)
                ix = int(m.group(1))
                if ix < len(history) :
                    current_command = history[ix]
                    run_sql(current_command)
                    history.append(current_command)
                    current_command = ""
                    continue
            if not current_command and re.search(r"^\s*\\x\s*?(.*)",_x_sin) :
                m = re.search(r"\\x\s*?(.+)",_x_sin)
                sqlfile = m.group(1)
                sqlfile = sqlfile.strip()
                sqlfile = sqlfile.strip(";")
                #stmt = open(os.path.expanduser(sqlfile),"r").read()
                history.append(_x_sin)
                #run_sql(stmt)
                run_sql(sqlfile)
                current_command = ""
                continue
            if not current_command and re.search(r"^\s*!\s*?(.*)",_x_sin) :
                history.append(_x_sin)
                m = re.search(r"!\s*?(.+)",_x_sin)
                excmd = m.group(1)
                excmd = excmd.strip()
                if not excmd :
                    excmd="echo"
                os.system(excmd)
                current_command = ""
                continue
            if not current_command and re.search(r"\s*\\q;",_x_sin) :
                return
            if not current_command and re.search(r"\s*\\hist\s*",_x_sin) :
                for ix, command in enumerate(history) :
                    print("# {} : {}".format(ix, command))
                current_command = ""
                continue
            current_command += _x_sin
            if re.search(r";\s*$",current_command) :
                run_sql(current_command)
                history.append(current_command)
                current_command = ""

    dbs = {}
    if os.path.isfile(os.path.expanduser(args.cfgfile)) :
        with open(os.path.expanduser(args.cfgfile),"r") as f :
            js = json.loads(f.read())
            PLUGINS.update(js.get("plugins",{}))
            for r in js.get("databases") :
                if "alias" in r and "URL" in r :
                    dbs[r["alias"]] = r["URL"]
    if dbs and args.db in dbs  :
        args.db = dbs[args.db]
    if "//" not in args.db :
        args.db = "sqlite+pysqlite:///"+args.db

    try :
        engine = create_engine(args.db,echo=args.debug)
        con = engine.connect()
    except :
        print(traceback.format_exc(),file=sys.stderr,flush=True)
        sys.exit(-1)

    refresh_tables(args.tables)
    if args.sql :
        run_sql(args.sql)
    else :
        interactive()

    con.close()

if __name__ == "__main__":
    xdb_main()
