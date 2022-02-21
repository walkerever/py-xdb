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
            _wordlist = "ABORT,ABS,ABSOLUTE,ACCESS,ACTION,ADA,ADD,ADMIN,AFTER,AGGREGATE,ALIAS,ALL,ALLOCATE,ALLOW,ALSO,ALTER,ALTERAND,ALWAYS,ANALYSE,ANALYZE,AND,ANY,ARE,ARRAY,ARRAY1,ARRAY_EXISTS1,AS,ASC,ASENSITIVE,ASSERTION,ASSIGNMENT,ASSOCIATE,ASUTIME,ASYMMETRIC,AT,ATOMIC,ATTRIBUTE,ATTRIBUTES,AUDIT,AUTHORIZATION,AUTO_INCREMENT,AUX,AUXILIARY,AVG,BACKWARD,BDB,BEFORE,BEGIN,BERKELEYDB,BERNOULLI,BETWEEN,BIGINT,BINARY,BIT,BIT_LENGTH,BITVAR,BLOB,BOOLEAN,BOTH,BREADTH,BTREE,BUFFERPOOL,BY,CACHE,CALL,CALLED,CAPTURE,CARDINALITY,CASCADE,CASCADED,CASE,CAST,CATALOG,CATALOG_NAME,CCSID,CEIL,CEILING,CHAIN,CHANGE,CHAR,CHARACTER,CHARACTERISTICS,CHARACTER_LENGTH,CHARACTERS,CHARACTER_SET_CATALOG,CHARACTER_SET_NAME,CHARACTER_SET_SCHEMA,CHAR_LENGTH,CHECK,CHECKED,CHECKPOINT,CLASS,CLASS_ORIGIN,CLOB,CLONE,CLOSE,CLUSTER,COALESCE,COBOL,COLLATE,COLLATION,COLLATION_CATALOG,COLLATION_NAME,COLLATION_SCHEMA,COLLECT,COLLECTION,COLLID,COLUMN,COLUMN_NAME,COLUMNS,COMMAND_FUNCTION,COMMAND_FUNCTION_CODE,COMMENT,COMMIT,COMMITTED,COMPLETION,CONCAT,CONDITION,CONDITION_NUMBER,CONNECT,CONNECTION,CONNECTION_NAME,CONSTRAINT,CONSTRAINT_CATALOG,CONSTRAINT_NAME,CONSTRAINTS,CONSTRAINT_SCHEMA,CONSTRUCTOR,CONTAINS,CONTENT,CONTINUE,CONVERSION,CONVERT,COPY,CORR,CORRESPONDING,COUNT,COVAR_POP,COVAR_SAMP,CREATE,CREATEDB,CREATEROLE,CREATEUSER,CROSS,CSV,CUBE,CUME_DIST,CURRENT,CURRENT_DATE,CURRENT_DEFAULT_TRANSFORM_GROUP,CURRENT_LC_CTYPE,CURRENT_PATH,CURRENT_ROLE,CURRENT_SCHEMA,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_TRANSFORM_GROUP_FOR_TYPE,CURRENT_USER,CURRVAL,CURSOR,CURSOR_NAME,CYCLE,DATA,DATABASE,DATABASES,DATE,DATETIME_INTERVAL_CODE,DATETIME_INTERVAL_PRECISION,DAY,DAY_HOUR,DAY_MINUTE,DAYS,DAY_SECOND,DBINFO,DEALLOCATE,DEC,DECIMAL,DECLARE,DEFAULT,DEFAULTS,DEFERRABLE,DEFERRED,DEFINED,DEFINER,DEGREE,DELAYED,DELETE,DELIMITER,DELIMITERS,DENSE_RANK,DEPTH,DEREF,DERIVED,DESC,DESCRIBE,DESCRIPTOR,DESTROY,DESTRUCTOR,DETERMINISTIC,DIAGNOSTICS,DICTIONARY,DISABLE,DISALLOW,DISCONNECT,DISPATCH,DISTINCT,DISTINCTROW,DIV,DO,DOCUMENT,DOMAIN,DOUBLE,DROP,DSSIZE,DYNAMIC,DYNAMIC_FUNCTION,DYNAMIC_FUNCTION_CODE,EACH,EDITPROC,ELEMENT,ELSE,ELSEIF,ENABLE,ENCLOSED,ENCODING,ENCRYPTED,ENCRYPTION,END,END-EXEC,END-EXEC2,ENDING,ENUM,EQUALS,ERASE,ERRORS,ESCAPE,ESCAPED,EVERY,EXCEPT,EXCEPTION,EXCLUDE,EXCLUDING,EXCLUSIVE,EXEC,EXECUTE,EXISTING,EXISTS,EXIT,EXP,EXPLAIN,EXTERNAL,EXTRACT,FALSE,FENCED,FETCH,FIELDPROC,FIELDS,FILTER,FINAL,FIRST,FLOAT,FLOOR,FOLLOWING,FOR,FORCE,FOREIGN,FORTRAN,FORWARD,FOUND,FREE,FREEZE,FROM,FULL,FULLTEXT,FUNCTION,FUSION,GENERAL,GENERATED,GEOMETRY,GET,GLOBAL,GO,GOTO,GRANT,GRANTED,GREATEST,GROUP,GROUPING,HANDLER,HASH,HAVING,HEADER,HELP,HIERARCHY,HIGH_PRIORITY,HOLD,HOST,HOUR,HOUR_MINUTE,HOURS,HOUR_SECOND,IDENTITY,IF,IGNORE,ILIKE,IMMEDIATE,IMMUTABLE,IMPLEMENTATION,IMPLICIT,IN,INCLUDING,INCLUSIVE,INCREMENT,INDEX,INDICATOR,INFILE,INFIX,INHERIT,INHERITS,INITIALIZE,INITIALLY,INNER,INNODB,INOUT,INPUT,INSENSITIVE,INSERT,INSTANCE,INSTANTIABLE,INSTEAD,INT,INTEGER,INTERSECT,INTERSECTION,INTERVAL,INTO,INVOKER,IS,ISNULL,ISOBID,ISOLATION,ITERATE,JAR,JOIN,KEEP,KEY,KEY_MEMBER,KEYS,KEY_TYPE,KILL,LABEL,LANCOMPILER,LANGUAGE,LARGE,LAST,LATERAL,LC_CTYPE,LEADING,LEAST,LEAVE,LEFT,LENGTH,LESS,LEVEL,LIKE,LIMIT,LINES,LISTEN,LN,LOAD,LOCAL,LOCALE,LOCALTIME,LOCALTIMESTAMP,LOCATION,LOCATOR,LOCATORS,LOCK,LOCKMAX,LOCKSIZE,LOGIN,LONG,LONGBLOB,LONGTEXT,LOOP,LOWER,LOW_PRIORITY,MAINTAINED,MAP,MASTER_SERVER_ID,MATCH,MATCHED,MATERIALIZED,MAX,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MEMBER,MERGE,MESSAGE_LENGTH,MESSAGE_OCTET_LENGTH,MESSAGE_TEXT,METHOD,MICROSECOND,MICROSECONDS,MIDDLEINT,MIN,MINUTE,MINUTEMINUTES,MINUTE_SECOND,MINVALUE,MOD,MODE,MODIFIES,MODIFY,MODULE,MONTH,MONTHS,MORE,MOVE,MRG_MYISAM,MULTISET,MUMPS,NAME,NAMES,NATIONAL,NATURAL,NCHAR,NCLOB,NESTING,NEW,NEXT,NEXTVAL,NO,NOCREATEDB,NOCREATEROLE,NOCREATEUSER,NOINHERIT,NOLOGIN,NONE,NORMALIZE,NORMALIZED,NOSUPERUSER,NOT,NOTHING,NOTIFY,NOTNULL,NOWAIT,NULL,NULLABLE,NULLIF,NULLS,NUMBER,NUMERIC,NUMPARTS,OBID,OBJECT,OCTET_LENGTH,OCTETS,OF,OFF,OFFSET,OIDS,OLD,ON,ONLY,OPEN,OPERATION,OPERATOR,OPTIMIZATION,OPTIMIZE,OPTION,OPTIONALLY,OPTIONS,OR,ORDER,ORDERING,ORDINALITY,ORGANIZATION,OTHERS,OUT,OUTER,OUTFILE,OUTPUT,OVER,OVERLAPS,OVERLAY,OVERRIDING,OWNER,PACKAGE,PAD,PADDED,PARAMETER,PARAMETER_MODE,PARAMETER_NAME,PARAMETER_ORDINAL_POSITION,PARAMETERS,PARAMETER_SPECIFIC_CATALOG,PARAMETER_SPECIFIC_NAME,PARAMETER_SPECIFIC_SCHEMA,PART,PARTIAL,PARTITION,PARTITIONED,PARTITIONING,PASCAL,PASSWORD,PATH,PERCENTILE_CONT,PERCENTILE_DISC,PERCENT_RANK,PERIOD,PIECESIZE,PLACING,PLAN,PLI,POSITION,POSTFIX,POWER,PRECEDING,PRECISION,PREFIX,PREORDER,PREPARE,PREPARED,PRESERVE,PREVVAL,PRIMARY,PRIOR,PRIQTY,PRIVILEGES,PROCEDURAL,PROCEDURE,PROGRAM,PSID,PUBLIC,PURGE,QUERY,QUERYNO,QUOTE,RANGE,RANK,READ,READS,REAL,RECHECK,RECURSIVE,REF,REFERENCES,REFERENCING,REFRESH,REGEXP,REGR_AVGX,REGR_AVGY,REGR_COUNT,REGR_INTERCEPT,REGR_R2,REGR_SLOPE,REGR_SXX,REGR_SXY,REGR_SYY,REINDEX,RELATIVE,RELEASE,RENAME,REPEAT,REPEATABLE,REPLACE,REQUIRE,RESET,RESIGNAL,RESTART,RESTRICT,RESULT,RESULT_SET_LOCATOR,RETURN,RETURNED_CARDINALITY,RETURNED_LENGTH,RETURNED_OCTET_LENGTH,RETURNED_SQLSTATE,RETURNS,REVOKE,RIGHT,RLIKE,ROLE,ROLLBACK,ROLLUP,ROLLUP1,ROUND_CEILING,ROUND_DOWN,ROUND_FLOOR,ROUND_HALF_DOWN,ROUND_HALF_EVEN,ROUND_HALF_UP,ROUND_UP,ROUTINE,ROUTINE_CATALOG,ROUTINE_NAME,ROUTINE_SCHEMA,ROW,ROW_COUNT,ROW_NUMBER,ROWS,ROWSET,RTREE,RULE,RUN,SAVEPOINT,SCALE,SCHEMA,SCHEMA_NAME,SCOPE,SCOPE_CATALOG,SCOPE_NAME,SCOPE_SCHEMA,SCRATCHPAD,SCROLL,SEARCH,SECOND,SECONDS,SECQTY,SECTION,SECURITY,SELECT,SELF,SENSITIVE,SEQUENCE,SERIALIZABLE,SERVER_NAME,SESSION,SESSION_USER,SET,SETOF,SETS,SHARE,SHOW,SIGNAL,SIMILAR,SIMPLE,SIZE,SMALLINT,SOME,SONAME,SOURCE,SPACE,SPATIAL,SPECIFIC,SPECIFIC_NAME,SPECIFICTYPE,SQL,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQLCODE,SQLERROR,SQLEXCEPTION,SQL_SMALL_RESULT,SQLSTATE,SQLWARNING,SQRT,SSL,STABLE,STANDARD,START,STARTING,STATE,STATEMENT,STATIC,STATISTICS,STAY,STDDEV_POP,STDDEV_SAMP,STDIN,STDOUT,STOGROUP,STORAGE,STORES,STRAIGHT_JOIN,STRICT,STRIPED,STRUCTURE,STYLE,SUBCLASS_ORIGIN,SUBLIST,SUBMULTISET,SUBSTRING,SUM,SUMMARY,SUPERUSER,SYMMETRIC,SYNONYM,SYSDATE,SYSID,SYSTEM,SYSTEM_USER,SYSTIMESTAMP,TABLE,TABLE_NAME,TABLES,TABLESAMPLE,TABLESPACE,TEMP,TEMPLATE,TEMPORARY,TERMINATE,TERMINATED,TEXT,THAN,THEN,TIES,TIME,TIMESTAMP,TIMEZONE_HOUR,TIMEZONE_MINUTE,TINYBLOB,TINYINT,TINYTEXT,TO,TOAST,TOP_LEVEL_COUNT,TRAILING,TRANSACTION,TRANSACTION_ACTIVE,TRANSACTIONS_COMMITTED,TRANSACTIONS_ROLLED_BACK,TRANSFORM,TRANSFORMS,TRANSLATE,TRANSLATION,TREAT,TRIGGER,TRIGGER_CATALOG,TRIGGER_NAME,TRIGGER_SCHEMA,TRIM,TRUE,TRUNCATE,TRUSTED,TYPE,TYPES,UESCAPE,UNBOUNDED,UNCOMMITTED,UNDER,UNDO,UNENCRYPTED,UNION,UNIQUE,UNKNOWN,UNLISTEN,UNLOCK,UNNAMED,UNNEST,UNSIGNED,UNTIL,UPDATE,UPPER,USAGE,USE,USER,USER_DEFINED_TYPE_CATALOG,USER_DEFINED_TYPE_CODE,USER_DEFINED_TYPE_NAME,USER_DEFINED_TYPE_SCHEMA,USER_RESOURCES,USING,VACUUM,VALID,VALIDATOR,VALIDPROC,VALUE,VALUES,VARBINARY,VARCHAR,VARCHARACTER,VARIABLE,VARIANT,VAR_POP,VAR_SAMP,VARYING,VCAT,VERBOSE,VERSIONING1,VIEW,VOLATILE,VOLUMES,WARNINGS,WHEN,WHENEVER,WHERE,WHILE,WIDTH_BUCKET,WINDOW,WITH,WITHIN,WITHOUT,WLM,WORK,WRITE,XMLCAST,XMLEXISTS,XMLNAMESPACES,XOR,YEAR,YEAR_MONTH,YEARS,ZEROFILL,ZONE".split(",")
            # mysql/postgresql
            try :
                lst = []
                results = con.execute("select table_schema from information_schema.tables union select table_name from information_schema.tables union select column_name from information_schema.columns")
                for r in results :
                    lst.append(r[0])
                _wordlist += lst
            except :
                pass
            # db2
            try :
                lst = []
                results = con.execute("select tabschema from syscat.tables union select tabname from syscat.tables union select colname from syscat.columns ")
                for r in results :
                    lst.append(r[0])
                _wordlist += lst
            except :
                pass
            _x_completer = WordCompleter(_wordlist,ignore_case=True)
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
                PIVOT_OUTPUT=False
                current_command = ""
                continue
            if not current_command and re.search(r"^\s*\\set\s+pivot\s*$",_x_sin) :
                PIVOT_OUTPUT=True
                WRAP_OUTPUT=False
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
