#%%
import cx_Oracle
import pandas as pd
import ftplib
import os
from datetime import datetime
from pathlib import Path
from pyunpack import Archive
from sqlalchemy import create_engine,types
import sqlite3
from tqdm import tqdm as tqdm_
from datpy.helpers import helper as hp
from datpy.filetool.output_file import logs
from munch import DefaultMunch

def runtime(func):
    def func_wrapper(*args, **kwargs):
        start = datetime.now()
        res = func(*args, **kwargs)
        stop = datetime.now()
        print("--> Finish in {}s".format(str(stop - start).split(".")[0])) 
        return res
    return func_wrapper

def showlog(show = 'log', level = 'Error'):
    level = level.lower()
    if show == 'log':
        if level == 'error':
            return hp.cfg['log'].error
        elif level == 'warning':
            return hp.cfg['log'].warning
        elif level == 'info':
            return hp.cfg['log'].info
        elif level == 'critical':
            return hp.cfg['log'].critical
    elif show == 'print':
        return print


class FtpServer:

    def __init__(self, configs_database):
        self.configs_database = configs_database
        self.connect()

    def connect(self):
        self.ftp = ftplib.FTP(self.configs_database.hostname, 
                            self.configs_database.username, 
                            self.configs_database.password)
        self.ftp.encoding = "utf-8"
        hp.cfg['log'].info(f'Success connecting to server {self.configs_database.hostname}')
        return self.ftp

    def listfile(self,folder=None):
        server = self.connect()
        if folder is not None:
            server.cwd(folder)
        return server.nlst()

    def listdir(self, folder = None, countfile = 0):
        server = self.connect()
        listdir = []
        if folder is not None:
            server.cwd(folder)
        currentdir = server.pwd()
        listfile = server.nlst()
        for f in listfile:
            path = currentdir + "/" + f
            try:
                server.cwd(path)
                append_file = self.listdir(folder = path, countfile = countfile)
                listdir += append_file
                countfile += len(append_file)
            except:
                listdir.append(path)
                countfile+=1
        print(f"Count files in ftp's folder \'{folder}\': ", countfile)
        hp.cfg['log'].info(f"Count files in ftp's folder '{folder}': {countfile}")
        return listdir

    def toFtpFile(self,filedirFTP, folderSaveLocal):
        return FtpFile(self.configs_database,filedirFTP, folderSaveLocal)


class FtpFile(FtpServer):

    def __init__(self, configs_database, filedirFTP, folderSaveLocal):
        super().__init__(configs_database)
        self.filedirFTP = filedirFTP
        self.folderLocal = folderSaveLocal
        self.filename = os.path.basename(self.filedirFTP)
        self.filedirLOCAL = os.path.join(self.folderLocal,self.filename)

    def getsize(self):
        a = self.connect()
        sizefile = a.size(self.filedirFTP)
        if sizefile > 1024**3:
            return "{:.2f} GB".format(sizefile/1024**3)
        elif sizefile > 1024**2:
            return "{:.2f} MB".format(sizefile/1024**2)
        elif sizefile > 1024:
            return "{:.2f} KB".format(sizefile/1024)
        else:
            return "{} bytes".format(sizefile)

    def checkFolder(self):
        Path(self.folderLocal).mkdir(parents=True, exist_ok=True)
        listfiles = os.listdir(self.folderLocal)
        for filename in listfiles:
            os.remove(os.path.join(self.folderLocal,filename))
            hp.cfg['log'].info(f'Removed {filename} in \'{self.folderLocal}\'')

    # @runtime
    def unpackFile(self,delZipFile=True):
        pathFile = self.filedirLOCAL
        try:
            Archive(pathFile,).extractall(os.path.dirname(pathFile))
            hp.cfg['log'].info(f'Unpacked file {self.filename}')
            os.remove(pathFile)
            hp.cfg['log'].info(f'Removed file {pathFile}')
        except:
            hp.cfg['log'].error(f'Failed to unpack file {self.filename}')
    
    def getlistdir(self):
        if os.path.splitext(self.filedirLOCAL)[1] in ['.zip','.rar','.gz','.bz2','.7z']:
            self.unpackFile()
        self.unpack_filelist = [os.path.join(self.folderLocal, i) for i in os.listdir(self.folderLocal)]
        hp.cfg['log'].info(f'Get all files {self.filename}')

    # @runtime
    def downloadFile(self):
        FTPdir = os.path.dirname(self.filedirFTP)
        sizefile = self.getsize()
        ftp = self.connect()
        ftp.cwd(FTPdir)
        assert (self.filename in ftp.nlst())
        with open(self.filedirLOCAL, 'wb') as fobj:
            ftp.retrbinary('RETR ' + self.filename, fobj.write)
            hp.cfg['log'].info(f'Downloaded {self.filedirFTP} ({sizefile})')

    def process(self, run_download= True):
        if run_download:
            self.checkFolder()
            self.downloadFile()
        self.getlistdir()

class Database:

    def __init__(self, configs_database, show = 'log'):
        self.configs_database = DefaultMunch.fromDict(configs_database)
        self.show = show
        self.connect()

    def connect(self):
        try:
            cf = self.configs_database
            if cf.type == 'oracle': # oracle
                self.conn = cx_Oracle.connect(user=cf.username,password=cf.password,dsn=f"{cf.hostname}:{cf.port}/{cf.service_name}")
                self.engine = create_engine(f'oracle+cx_oracle://{cf.username}:{cf.password}@{cf.hostname}:{cf.port}/?service_name={cf.service_name}')
                showlog(show = self.show, level = 'info')(f"Success connecting to database {cf.hostname}:{cf.port}/{cf.service_name}")
            elif cf.type == 'sqlite3':
                self.conn = sqlite3.connect(cf.path)
                self.engine = sqlite3.connect(cf.path)
                showlog(show = self.show, level = 'info')(f"Success connecting to database {cf.path}")
            
        except Exception as e:
            showlog(show = self.show, level = 'error')('Fail to connect to database !')
            raise e

    # @runtime
    def drop(self, tablename,schema=None):
        # Drop table if exists
        cursor = self.conn.cursor() 
        tablename = "{}.{}".format(schema,tablename) if schema is not None else tablename
        showlog(show = self.show, level = 'warning')(f'Droping {tablename.upper()} table if exists.')
        cursor.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tablename.upper()}'; EXCEPTION WHEN OTHERS THEN NULL; END;")
        showlog(show = self.show, level = 'warning')(f'Droped {tablename.upper()} table if exists.')

    def create(self, tablename:str, typeCol:dict, schema = None):

        def check_exists_table(tablename,schema,conn):
            sql = f"""
            select count(*) from user_tables 
            where table_name = '{tablename}'
            and tablespace_name= '{schema}'
            """
            cnt = pd.read_sql_query(sql,conn).iloc[0,0]
            if cnt == 0:
                showlog(show = self.show, level = 'warning')(f'There are no {tablename.upper()} table')
                return False
            else:
                # hp.cfg['log'].info(f'There are no {tablename.upper()} table')
                return True

        if check_exists_table(tablename,schema,self.engine) == False:
            try:
                tablename = ("{}.{}".format(schema,tablename) if schema is not None else tablename).upper()
                cursor = self.conn.cursor() 
                schemaCol = ", ".join([ "{} {}".format(i,typeCol[i]) for i in typeCol.keys()])  
                cursor.execute(f"CREATE TABLE {tablename} ({schemaCol})" )
                showlog(show = self.show, level = 'info')(f'Created {tablename.upper()} table in {schema.upper()}')
                return True
            except:
                showlog(show = self.show, level = 'error')(f'Fail to created {tablename.upper()} table in {schema.upper()}')

    def getdtype(dataSchema):
        def convert_tool(x:str):
            if x.lower() == 'date':
                return types.DATE()
            elif x.lower().startswith('varchar2'):
                lenght_varchar2 = int(x[x.index("(")+1:x.index(")")])
                return types.VARCHAR(lenght_varchar2)
            elif 'float' in x.lower() :
                return types.FLOAT()
            elif 'integer' in x.lower() :
                return types.INTEGER()
        return {i:convert_tool(dataSchema[i]) for i in dataSchema.keys()}

    # @logs(logger = hp.cfg['log'])
    def upload(self,data,dataSchema,tablename:str, schema = None,chunksize = 5000,if_exists = 'append',filename = None):
        try:
            dty = Database.getdtype(dataSchema)
            data.to_sql(tablename.lower(),schema = schema, con = self.engine, if_exists = if_exists, 
                        chunksize = chunksize, index=False, dtype = dty)
            # hp.cfg['log'].info(f"Uploaded ('{if_exists}') data to {tablename.upper()} table in {schema.upper()} from {data.index[0]} to {data.index[-1]}")
        except Exception as e:
            showlog(show = self.show, level = 'error')(f"Fail to upload data {filename} from {data.index[0]} to {data.index[-1]} with error: {e}")


    def access(self,toUser ,tablename, access = 'select', schema = None):
        """
        grant select/insert/update/delete on <schema>.<table_name> to <username>;
        """
        cursor = self.conn.cursor()
        tablename = "{}.{}".format(schema,tablename) if schema is not None else tablename
        cursor.execute(f"""grant {access} on {tablename} to {toUser};""")
        self.conn.commit()
        cursor.close()
        print(f'Set {toUser} to {access} in {tablename} !')

    def createIndex(self,indexname ,tablename, cols , schema = None):
        """
        CREATE INDEX <indexname> ON <schema.tablename> (cols);
        """
        cursor = self.conn.cursor()
        cols_list = cols if type(cols) != list else ", ".join(cols)
        tablename = "{}.{}".format(schema,tablename) if schema is not None else tablename
        cursor.execute(f"""CREATE INDEX {indexname} ON {tablename} ({cols_list});""")
        self.conn.commit()
        cursor.close()
        # conn.close()
        print(f'Set {indexname} as index to {cols_list} in {tablename} !')

    @runtime
    def read(self, table_name: str = None , col_name = "*" ,
        offset_rows : int = 0, n_records : int = -1, chunksize : int = None  , sql : str=None):

        if (table_name is None) and (sql is None):
            if type(self.conn)==cx_Oracle.Connection:
                return pd.read_sql_query("SELECT OWNER,TABLE_NAME,TABLESPACE_NAME  FROM all_tables",self.engine)
            else:
                return pd.read_sql_query("SELECT *  FROM sqlite_master",self.engine)

        if type(self.conn)==cx_Oracle.Connection:
            offset_clause= " offset {} rows ".format(offset_rows)
            num_records_clause = "" if n_records == -1 else " fetch next {} rows only".format(n_records)
            combine_clause = offset_clause + num_records_clause
        else: # sqlite3
            offset_clause= "" if offset_rows==0 else " offset {} ".format(offset_rows)
            num_records_clause = "limit -1" if n_records == -1 else " limit {} ".format(n_records)
            combine_clause =  num_records_clause + offset_clause

        if sql is None:
            cols=col_name if type(col_name)==str else ", ".join(col_name)
            sql="""
            select {} from {} {}
            """.format(cols,table_name,combine_clause)

        res=pd.read_sql_query(sql=sql, con=self.engine,chunksize=chunksize)
        if chunksize is not None:
            res = tqdm_(res)
        # print("Bảng {} offset {} dòng, {} records".format(table_name,offset_rows,n_records) + ("" if chunksize is None else ", chunksize {}".format(chunksize)))
        return res
