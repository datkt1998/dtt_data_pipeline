import re
import numpy as np
import pandas as pd
from unidecode import unidecode
from datpy.vmg.cleaning import text
from datpy.helpers import helper as hp
from tqdm import tqdm
import os

class CleanProcess:

    def __init__(self):
        pass

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
            # raise
            hp.cfg['log'].critical(f"File '{self.filename}' has the failure in data columns!!!")

    def check_rangeIndex(self,df):
        if self.rangeIndex is None:
            return True
        if self.rangeIndex is not None:
            range_index = self.rangeIndex[0]
            if df.index[0] == range_index[0]:
                if self.chunksize != (range_index[1] - range_index[0] +1):
                    raise f"Chunksize is {self.chunksize}, but rangeIndex is from {range_index[0]} to {range_index[1]}"
                else:
                    self.rangeIndex.pop(0)
                    return True
            else:
                return False

    def cleaning_pandas(self):
        pass

    def process(self,oracle_db=None):
        if self.validate_col:
            created = oracle_db.create(self.tablename, self.dataSchema, self.schema)
            # if created:
            #     oracle_db.createIndex('idx' ,self.tablename, cols = ['PHONE_NUMBER', 'IDCARD'] , schema = self.schema)
            hp.cfg['log'].info(f'Processing {self.filename} to {self.tablename}')
            with self.datachunk as f_chunk:
                cnt = 0
                for df in tqdm(f_chunk,desc = self.filename, position=2, leave=False):
                    cnt+=1
                    if self.rangeIndex == []:
                        break
                    if self.check_rangeIndex(df):
                        res = self.cleaning_pandas(df)
                        if type(res) != bool:
                            oracle_db.upload(res,self.dataSchema ,self.tablename, self.schema ,chunksize = 10000, filename = self.filename)

def importMonthInfo(filedir, typedata):
    if typedata == 'telco_info_viettel':
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
