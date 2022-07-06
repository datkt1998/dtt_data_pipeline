from datpy.vmg.cleaning import text, address, Phone, IDcard, unidecode
from datpy.vmg.codecrypt import encrypt_df
import pandas as pd
import re
from tqdm import tqdm
import os
from datpy.helpers import helper as hp
# from sqlalchemy import types
# from datpy.database.connection import Database
# from sqlalchemy.sql.sqltypes import VARCHAR
from datpy.filetool.output_file import logs

class MobifoneInfo:
    name = 'Mobifone'
    source = "FTP_SERVER"
    def __init__(self,filedir,cfg):
        self.schema = cfg['schema']
        self.tablename = cfg['tablename']
        self.dataSchema = cfg['dataSchema']
        self.cols = self.dataSchema.keys()
        self.chunksize = 10000
        self.filename = os.path.basename(filedir)
        self.datacols = sorted(pd.read_csv(filedir, on_bad_lines='skip',sep = "|", nrows= 1).columns.tolist())
        self.datachunk = pd.read_csv(filedir, on_bad_lines='skip',sep = "|", chunksize =self.chunksize)
        self.rename_dict = {'thuebao':'PHONE_NUMBER',
                            'socmnd':'IDCARD',
                            'hoten':'FULLNAME',
                            'ngaysinh':'DOB',
                            'diachi':'ADDRESS',
                            'loaitb':'PAY_TYPE',
                            'ngaykichhoat':'ACTIVE_DATE',
                            'ngaythaydoi':'UPDATE_DATE',
                            'email':'EMAIL'}
        self.import_month = MobifoneInfo.get_IMPORT_MONTH(cfg['importMonth'])
        self.validate_inputdata()

    def get_IMPORT_MONTH(x):
        try:
            if re.match("[0-9]{6}", str(x)):
                return str(x)
            else:
                return pd.to_datetime(str(x)).strftime('%Y%m')
        except:
            raise "check lại import month"

    def get_PAY_TYPE(x):
        x = unidecode(x)
        if len([i for i in ['pre','truoc'] if i in x])>0:
            return 'prepaid'
        if len([i for i in ['post','sau'] if i in x])>0:
            return 'postpaid'
        return 'unknown'

    def validate_inputdata(self):
        self.validate_col = (self.datacols == sorted(list(self.rename_dict.keys())))
        if self.validate_col == False:
            hp.cfg['log'].critical(f"File '{self.filename}' has the failure in data columns!!!")

    def cleaning_pandas(data,rename_dict,import_month,cols,filename=None):
        try:
            data = data\
                .rename(columns=rename_dict)\
                .applymap(lambda x: text(x).clean())\
                .assign(
                    PHONE_NUMBER = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').cleaned),
                    IDCARD = lambda t: t['IDCARD'].map(lambda x: IDcard(x).standardize()),
                    IDCARD_TYPE = lambda t: t['IDCARD'].map(lambda x: IDcard(x).typeIDstandard()),
                    FULLNAME = lambda t: t['FULLNAME'].map(lambda x: str(x).title(),na_action='ignore'),
                    PROVINCE = lambda t: t['ADDRESS'].map(address.get_province,na_action='ignore'),
                    PAY_TYPE = lambda t: t['PAY_TYPE'].map(MobifoneInfo.get_PAY_TYPE,na_action='ignore'),
                    DOB = lambda t: pd.to_datetime(t['DOB'], dayfirst=True, errors= 'coerce'),
                    ACTIVE_DATE = lambda t: pd.to_datetime(t['ACTIVE_DATE'], dayfirst=True, errors= 'coerce'),
                    UPDATE_DATE = lambda t: pd.to_datetime(t['UPDATE_DATE'], dayfirst=True, errors= 'coerce'),
                    SUB_TYPE = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').typephone),
                    CARRIER =MobifoneInfo.name,
                    IMPORT_MONTH = import_month,
                    SOURCE = MobifoneInfo.source)
            encrypt_df(data,'PHONE_NUMBER','IDCARD')
            return data.reindex(cols, axis = 1)
        except Exception as e:
            hp.cfg['log'].error(f"Fail to cleaning data {filename} from {data.index[0]} to {data.index[-1]} with error: {e}")
            return False

    def process(self,oracle_db=None):
        if self.validate_col:
            oracle_db.create(self.tablename, self.dataSchema, self.schema)
            for df in tqdm(self.datachunk,desc = self.filename, position=2, leave=False):
                hp.cfg['log'].info(f'Processing {self.filename} to {self.tablename}')
                res = MobifoneInfo.cleaning_pandas(df,self.rename_dict,self.import_month,cols = self.cols , filename = self.filename)
                if type(res) != bool:
                    oracle_db.upload(res,self.dataSchema ,self.tablename, self.schema ,chunksize = 10000, filename = self.filename)



class ViettelInfo:
    name = 'Viettel'
    def __init__(self,filedir= None,cfg = None):
        self.schema = cfg['schema']
        self.tablename = cfg['tablename']
        self.dataSchema = cfg['dataSchema']
        self.cols = self.dataSchema.keys()
        self.chunksize = 10000
        if self.filedir is not None:
            self.filename = os.path.basename(filedir)
            self.datacols = sorted(pd.read_csv(filedir, on_bad_lines='skip',sep = "|", nrows= 1).columns.tolist())
            self.datachunk = pd.read_csv(filedir, on_bad_lines='skip',sep = "|", chunksize =self.chunksize)
        else:
            self.filename = cfg['folder_config'].db_source.tablename
            self.datacols = sorted(cfg['source_oracle_server'].read(table_name=self.filename, n_records = 1).columns.tolist())
            self.datachunk = cfg['source_oracle_server'].read(table_name=self.filename, chunksize = self.chunksize)

        self.rename_dict = {'MSISDN':'PHONE_NUMBER',
                            'BIRTH_DATE':"DOB",
                            'QUOC_TICH':"NATIONALITY",
                            'LOAI_GIAY_TO_TB':'IDCARD_TYPE',
                            'ID_NO':'IDCARD',
                            'NGAY_CAP':'IDCARD_ISSUEDATE',
                            'NOI_CAP':'IDCARD_ADDRESS',
                            'ADDRESS':'ADDRESS',
                            'LOAI_THUE_BAO':'',
                            'CONG_NGHE_TB':'',
                            'MA_GOI_CUOC':'',
                            'ACTIVE_DATE':'',
                            'MOBILE_TYPE':'',
                            'SOURCE':'',
                            'UPDATE_DATE':'',
                            'IMPORT_MONTH':''}
        self.import_month = MobifoneInfo.get_IMPORT_MONTH(cfg['importMonth'])
        self.validate_inputdata()

    def get_IMPORT_MONTH(x):
        try:
            if re.match("[0-9]{6}", str(x)):
                return str(x)
            else:
                return pd.to_datetime(str(x)).strftime('%Y%m')
        except:
            raise "check lại import month"

    def get_PAY_TYPE(x):
        x = unidecode(x)
        if len([i for i in ['pre','truoc'] if i in x])>0:
            return 'prepaid'
        if len([i for i in ['post','sau'] if i in x])>0:
            return 'postpaid'
        return 'unknown'

    def validate_inputdata(self):
        self.validate_col = (self.datacols == sorted(list(self.rename_dict.keys())))
        if self.validate_col == False:
            hp.cfg['log'].critical(f"File '{self.filename}' has the failure in data columns!!!")

    def cleaning_pandas(data,rename_dict,import_month,cols,filename=None):
        try:
            data = data\
                .rename(columns=rename_dict)\
                .applymap(lambda x: text(x).clean())\
                .assign(
                    PHONE_NUMBER = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').cleaned),
                    IDCARD = lambda t: t['IDCARD'].map(lambda x: IDcard(x).standardize()),
                    IDCARD_TYPE = lambda t: t['IDCARD'].map(lambda x: IDcard(x).typeIDstandard()),
                    FULLNAME = lambda t: t['FULLNAME'].map(lambda x: str(x).title(),na_action='ignore'),
                    PROVINCE = lambda t: t['ADDRESS'].map(address.get_province,na_action='ignore'),
                    PAY_TYPE = lambda t: t['PAY_TYPE'].map(MobifoneInfo.get_PAY_TYPE,na_action='ignore'),
                    DOB = lambda t: pd.to_datetime(t['DOB'], dayfirst=True, errors= 'coerce'),
                    ACTIVE_DATE = lambda t: pd.to_datetime(t['ACTIVE_DATE'], dayfirst=True, errors= 'coerce'),
                    UPDATE_DATE = lambda t: pd.to_datetime(t['UPDATE_DATE'], dayfirst=True, errors= 'coerce'),
                    SUB_TYPE = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').typephone),
                    CARRIER =MobifoneInfo.name,
                    IMPORT_MONTH = import_month
                    )
            encrypt_df(data,'PHONE_NUMBER','IDCARD')
            return data.reindex(cols, axis = 1)
        except Exception as e:
            hp.cfg['log'].error(f"Fail to cleaning data {filename} from {data.index[0]} to {data.index[-1]} with error: {e}")
            return False

    def process(self,oracle_db=None):
        if self.validate_col:
            oracle_db.create(self.tablename, self.dataSchema, self.schema)
            for df in tqdm(self.datachunk,desc = self.filename, position=2, leave=False):
                hp.cfg['log'].info(f'Processing {self.filename} to {self.tablename}')
                res = MobifoneInfo.cleaning_pandas(df,self.rename_dict,self.import_month,cols = self.cols , filename = self.filename)
                if type(res) != bool:
                    oracle_db.upload(res,self.dataSchema ,self.tablename, self.schema ,chunksize = 10000, filename = self.filename)
