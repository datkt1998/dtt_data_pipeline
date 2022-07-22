import re
import numpy as np
import pandas as pd
from unidecode import unidecode
from datpy.vmg.cleaning import text
from datpy.helpers import helper as hp
from tqdm import tqdm
import os
from concurrent.futures import ProcessPoolExecutor,as_completed,ThreadPoolExecutor

import csv
from datpy.database import connection as cn

class CleanProcess:

    def __init__(self):
        pass

    def getDataReader(self):
        if self.filedir is not None:
            filename = os.path.basename(self.filedir)
            datacols = sorted(pd.read_csv(self.filedir, on_bad_lines='skip',sep = self.sep, nrows= 1,quoting=csv.QUOTE_NONE).columns.tolist())
            datachunk = pd.read_csv(self.filedir, on_bad_lines='skip',sep = self.sep, chunksize =self.chunksize,quoting=csv.QUOTE_NONE) 
            # ,quotechar='"', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        else: # oracle
            filename = self.cfg['folder_config'].db_source.tablename
            datacols = sorted(self.cfg['source_oracle_server'].read(table_name=filename, n_records = 1).columns.tolist())
            datachunk = self.cfg['source_oracle_server'].read(table_name=filename, chunksize = self.chunksize)
        return filename, datacols, datachunk

    def get_IMPORT_MONTH(x):
        try:
            if re.match("[0-9]{6}", str(x)):
                return str(x)
            else:
                return pd.to_datetime(str(x)).strftime('%Y%m')
        except:
            return np.datetime64('NaT')

    def validate_inputdata(self):
        a = sorted([i.upper() for i in self.datacols])
        b = sorted([i.upper() for i in self.rename_dict.keys()])
        self.validate_col = ([e for e in b if e in a] == b) or (([e for e in a if e in b] == a))
        if self.validate_col == False:
            hp.cfg['log'].critical(f"File '{self.filename}' has the failure in data columns!!!")

    def check_rangeIndex(self,df):
        if self.rangeIndex == []:
            return False
        if self.rangeIndex is None:
            return True
        if self.rangeIndex is not None:
            rangeIndex_check = [df.index[0], df.index[-1]]
            if rangeIndex_check in self.rangeIndex:
                self.rangeIndex.remove(rangeIndex_check)
                return True
            else:
                return False

    def cleaning_pandas(self):
        pass

    def subprocess(self, df, oracle_db=None):
        if self.check_rangeIndex(df):
            res = self.cleaning_pandas(df)
            if type(res) != bool:
                oracle_db.upload(res,self.dataSchema ,self.tablename, self.schema ,chunksize = 10000, filename = self.filename)
    
    @cn.runtime
    def process(self,oracle_db=None):
        if self.validate_col:
            created = oracle_db.create(self.tablename, self.dataSchema, self.schema)
            hp.cfg['log'].info(f'Processing {self.filename} to {self.tablename}')
            with self.datachunk as f_chunk:
                for df in tqdm(f_chunk,desc = self.filename, position=2, leave=False):
                    if self.check_rangeIndex(df):
                        res = self.cleaning_pandas(df)
                        if type(res) != bool:
                            oracle_db.upload(res,self.dataSchema ,self.tablename, self.schema ,chunksize = 10000, filename = self.filename)


def importMonthInfo(filedir, typedata):
    if typedata == 'telco_bill_viettel':
        split_path = filedir.split('/')
        assert split_path[-2].lower() == 'viettel'
        filename = split_path[-1]
        if 'tt' in filename:
            paytype = 'pre_paid'
        elif 'ts' in filename:
            paytype = 'post_paid'
        else:
            paytype = np.nan
        month = re.findall("t[0-9]{1,2}", filename.lower())[0][1:]
        year = split_path[-3]
        importmonth = "{:04.0f}{:02.0f}".format(int(year),int(month))
        return (importmonth, paytype)

    elif typedata == 'telco_bill_vinaphone':
        split_path = filedir.split('/')
        assert 'vina' in  split_path[-2].lower()
        filename = split_path[-1]
        month = re.findall("t[0-9]{1,2}", filename.lower())[0][1:]
        year = split_path[-3]
        importmonth = "{:04.0f}{:02.0f}".format(int(year),int(month))
        return importmonth
