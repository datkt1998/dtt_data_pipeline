from datpy.vmg.cleaning import text, address, Phone, IDcard, unidecode
from datpy.vmg.codecrypt import encrypt_df, decrypt_df
import pandas as pd
import re
from tqdm import tqdm
import os
from datpy.helpers import helper as hp
import numpy as np
# from sqlalchemy import types
# from datpy.database.connection import Database
# from sqlalchemy.sql.sqltypes import VARCHAR
from datpy.filetool.output_file import logs
# import csv


class TelcoInfo:

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

    def get_PAY_TYPE(x):
        x = unidecode(text(x).clean()).lower()
        if len([i for i in ['pre','truoc','tt'] if i in x])>0:
            return 'prepaid'
        elif len([i for i in ['post','sau','ts'] if i in x])>0:
            return 'postpaid'
        else :
            return np.nan

    def convert_gender(x):
        if x != x:
            return np.nan
        x = unidecode(text(x).clean()).lower()
        if x in ['m', 'male','nam']:
            return 'M'
        elif x in ['f', 'female','nu']:
            return 'F'
        else:
            return np.nan

    def validate_inputdata(self):
        a = sorted([i.upper() for i in self.datacols])
        b = sorted([i.upper() for i in self.rename_dict.keys()])
        self.validate_col = ([e for e in b if e in a] == b)
        if self.validate_col == False:
            # raise
            hp.cfg['log'].critical(f"File '{self.filename}' has the failure in data columns!!!")

    def cleaning_pandas(self):
        pass

    def process(self,oracle_db=None):
        if self.validate_col:
            created = oracle_db.create(self.tablename, self.dataSchema, self.schema)
            # if created:
            #     oracle_db.createIndex('idx' ,self.tablename, cols = ['PHONE_NUMBER', 'IDCARD'] , schema = self.schema)
            hp.cfg['log'].info(f'Processing {self.filename} to {self.tablename}')
            for df in tqdm(self.datachunk,desc = self.filename, position=2, leave=False):
                res = self.cleaning_pandas(df)
                # raise
                if type(res) != bool:
                    oracle_db.upload(res,self.dataSchema ,self.tablename, self.schema ,chunksize = 10000, filename = self.filename)



class MobifoneInfo(TelcoInfo):
    name = 'Mobifone'
    source = "VENDOR_FTP_FILE"
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

    def cleaning_pandas(self,data):
        try:
            data = data\
                .rename(columns=self.rename_dict)\
                .applymap(lambda x: text(x).clean())\
                .assign(
                    PHONE_NUMBER = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').cleaned),
                    IDCARD = lambda t: t['IDCARD'].map(lambda x: IDcard(x).standardize()),
                    IDCARD_TYPE = lambda t: t['IDCARD'].map(lambda x: IDcard(x).typeIDstandard()),
                    FULLNAME = lambda t: t['FULLNAME'].map(lambda x: str(x).title(),na_action='ignore'),
                    ADDRESS = lambda t: t['ADDRESS'].map(lambda x: str(x).title(),na_action='ignore'),
                    PROVINCE = lambda t: t['ADDRESS'].map(address.get_province,na_action='ignore'),
                    PAY_TYPE = lambda t: t['PAY_TYPE'].map(MobifoneInfo.get_PAY_TYPE,na_action='ignore'),
                    DOB = lambda t: pd.to_datetime(t['DOB'], dayfirst=True, errors= 'coerce'),
                    ACTIVE_DATE = lambda t: pd.to_datetime(t['ACTIVE_DATE'], dayfirst=True, errors= 'coerce'),
                    UPDATE_DATE = lambda t: pd.to_datetime(t['UPDATE_DATE'], dayfirst=True, errors= 'coerce'),
                    SUB_TYPE = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').typephone),
                    CARRIER =MobifoneInfo.name,
                    IMPORT_MONTH = self.import_month,
                    SOURCE = MobifoneInfo.source)
            encrypt_df(data,'PHONE_NUMBER','IDCARD')
            return data.reindex(self.cols, axis = 1)
        except Exception as e:
            hp.cfg['log'].error(f"Fail to cleaning data {self.filename} from {data.index[0]} to {data.index[-1]} with error: {e}")
            return False




class ViettelInfo(TelcoInfo):
    name = 'Viettel'
    def __init__(self,filedir= None,cfg = None):
        self.schema = cfg['schema']
        self.tablename = cfg['tablename']
        self.dataSchema = cfg['dataSchema']
        self.cols = self.dataSchema.keys()
        self.chunksize = 10000
        self.filedir = filedir
        self.import_month = cfg['importMonth']
        if self.filedir is not None:
            self.filename = os.path.basename(filedir)
            self.datacols = sorted(pd.read_csv(filedir, on_bad_lines='skip',sep = "|", nrows= 1).columns.tolist())
            self.datachunk = pd.read_csv(filedir, on_bad_lines='skip',sep = "|", chunksize =self.chunksize)
        else: # oracle
            self.filename = cfg['folder_config'].db_source.tablename
            self.datacols = sorted(cfg['source_oracle_server'].read(table_name=self.filename, n_records = 1).columns.tolist())
            self.datachunk = cfg['source_oracle_server'].read(table_name=self.filename, chunksize = self.chunksize)

        self.rename_dict = {'MSISDN':'PHONE_NUMBER',
                            'NAME': 'FULLNAME',
                            'BIRTH_DATE':"DOB",
                            'QUOC_TICH':"NATIONALITY",
                            'LOAI_GIAY_TO_TB':'IDCARD_TYPE',
                            'ID_NO':'IDCARD',
                            'NGAY_CAP':'IDCARD_ISSUEDATE',
                            'NOI_CAP':'IDCARD_ADDRESS',
                            'ADDRESS':'ADDRESS',
                            'LOAI_THUE_BAO':'PAY_TYPE',
                            'CONG_NGHE_TB':'SIM_TYPE',
                            'MA_GOI_CUOC':'PACKAGE_TYPE',
                            'ACTIVE_DATE':'ACTIVE_DATE',
                            'SOURCE':'SOURCE',
                            'UPDATE_DATE':'UPDATE_DATE',
                            'IMPORT_MONTH':'IMPORT_MONTH'}
        self.rename_dict = {i.upper():self.rename_dict[i] for i in self.rename_dict}
        self.validate_inputdata()

    def cleaning_pandas(self,data):
        try:
            data.columns = [i.upper() for i in data.columns]
            data = data.rename(columns=self.rename_dict)
            decrypt_df(data,'PHONE_NUMBER','IDCARD')
            data = data\
                .applymap(lambda x: text(x).clean())\
                .assign(
                    PHONE_NUMBER = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').cleaned),
                    FULLNAME = lambda t: t['FULLNAME'].map(lambda x: str(x).title(),na_action='ignore'),
                    DOB = lambda t: pd.to_datetime(t['DOB'], format="%Y%m%d", errors= 'coerce'),
                    NATIONALITY = lambda t: t['NATIONALITY'].map(lambda x: text.remove_punctuation(unidecode(str(x))).title(),na_action='ignore'),
                    IDCARD = lambda t: t['IDCARD'].map(lambda x: IDcard(x).standardize()),
                    IDCARD_TYPE = lambda t: t['IDCARD'].map(lambda x: IDcard(x).typeIDstandard()),
                    IDCARD_ADDRESS = lambda t: t['IDCARD_ADDRESS'].map(lambda x: str(x).title(),na_action='ignore'),
                    IDCARD_ISSUEDATE = lambda t: pd.to_datetime(t['IDCARD_ISSUEDATE'], format="%Y%m%d", errors= 'coerce'),
                    ADDRESS = lambda t: t['ADDRESS'].map(lambda x: str(x).title(),na_action='ignore'),
                    PROVINCE = lambda t: t['ADDRESS'].map(address.get_province,na_action='ignore'),
                    PAY_TYPE = lambda t: t['PAY_TYPE'].map(ViettelInfo.get_PAY_TYPE,na_action='ignore'),
                    SIM_TYPE = lambda t: t['SIM_TYPE'].map(lambda x: text.remove_punctuation(unidecode(str(x).lower())),na_action='ignore'),
                    PACKAGE_TYPE = lambda t: t['PACKAGE_TYPE'].map(lambda x: text.remove_punctuation(unidecode(str(x).upper())),na_action='ignore'),
                    ACTIVE_DATE = lambda t: pd.to_datetime(t['DOB'], format="%Y%m%d", errors= 'coerce'),
                    UPDATE_DATE = lambda t: pd.to_datetime(t['UPDATE_DATE'], format="%Y%m", errors= 'coerce'),
                    SUB_TYPE = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').typephone),
                    CARRIER =ViettelInfo.name,
                    IMPORT_MONTH = ((lambda t: t['IMPORT_MONTH'].map(ViettelInfo.get_IMPORT_MONTH)) if self.import_month is None else self.import_month),
                    SOURCE = lambda t: t['SOURCE'].map(lambda x: "VENDOR_FTP_FILE" if x == "VT" else "EKYC")
                    )
            encrypt_df(data,'PHONE_NUMBER','IDCARD')
            return data.reindex(self.cols, axis = 1)
        except Exception as e:
            hp.cfg['log'].error(f"Fail to cleaning data {self.filename} from {data.index[0]} to {data.index[-1]} with error: {e}")
            return False




class VinaphoneInfo(TelcoInfo):
    name = 'Vinaphone'
    def __init__(self,filedir= None,cfg = None):
        self.schema = cfg['schema']
        self.tablename = cfg['tablename']
        self.dataSchema = cfg['dataSchema']
        self.cols = self.dataSchema.keys()
        self.chunksize = 10000
        self.filedir = filedir
        if self.filedir is not None:
            self.filename = os.path.basename(filedir)
            self.datacols = sorted(pd.read_csv(filedir, on_bad_lines='skip',sep = "|", nrows= 1).columns.tolist())
            self.datachunk = pd.read_csv(filedir, on_bad_lines='skip',sep = "|", chunksize =self.chunksize)
            self.import_month = cfg['importMonth'][self.filename]
        else:
            self.filename = cfg['folder_config'].db_source.tablename
            self.datacols = sorted(cfg['source_oracle_server'].read(table_name=self.filename, n_records = 1).columns.tolist())
            self.datachunk = cfg['source_oracle_server'].read(table_name=self.filename, chunksize = self.chunksize)
            self.import_month = None

        self.rename_dict = {'TEN': 'FULLNAME',
                            'NGAY_SINH':"DOB",
                            'DIA_CHI':"ADDRESS",
                            'LOAI_MAY':'PHONE_TYPE',
                            'DONG_MAY':'PHONE_BRAND',
                            'NGAY_KICH_HOAT':'ACTIVE_DATE',
                            'GOI_SU_DUNG':'PACKAGE_TYPE',
                            'THOI_GIAN_ACTIVE_GOI':'UPDATE_DATE',
                            'LOAI_TB':'PAY_TYPE',
                            'TKC':'TKC',
                            'DATA_USG_MB':'DATA_USG_MB',
                            'NOIDK_TINH':'NOIDK_TINH',
                            'TINH':'PROVINCE',
                            'QUAN_HUYEN':'QUAN_HUYEN',
                            'PHUONG_XA':'PHUONG_XA',
                            'MSISDN':"PHONE_NUMBER", 
                            'SO_CMT':"IDCARD",
                            'GIOI_TINH':"GENDER", 
                            'NHA_SAN_XUAT':"NHA_SAN_XUAT", 
                            'NGAY_CAP_CMT':"IDCARD_ISSUEDATE"}
        self.rename_dict = {i.upper():self.rename_dict[i] for i in self.rename_dict}
        self.validate_inputdata()

    def cleaning_pandas(self,data):
        try:
            data.columns = [i.upper() for i in data.columns]
            data = data.rename(columns=self.rename_dict)\
                .applymap(lambda x: text(x).clean())\
                .assign(
                    PHONE_NUMBER = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').cleaned),
                    FULLNAME = lambda t: t['FULLNAME'].map(lambda x: str(x).title(),na_action='ignore'),
                    DOB = lambda t: pd.to_datetime(t['DOB'].map(text.remove_punctuation), format="%Y%m%d", errors= 'coerce'),
                    ADDRESS = lambda t: t['ADDRESS'].map(lambda x: str(x).title(),na_action='ignore'),
                    PHONE_BRAND = lambda t: t['PHONE_BRAND'].fillna(t['NHA_SAN_XUAT']).map(lambda x: str(x).title(),na_action='ignore'),
                    IDCARD = lambda t: t['IDCARD'].map(lambda x: IDcard(x).standardize()),
                    GENDER = lambda t: t['GENDER'].map(TelcoInfo.convert_gender),
                    IDCARD_TYPE = lambda t: t['IDCARD'].map(lambda x: IDcard(x).typeIDstandard()),
                    IDCARD_ISSUEDATE = lambda t: pd.to_datetime(t['IDCARD_ISSUEDATE'].map(text.remove_punctuation), format="%Y%m%d", errors= 'coerce'),
                    PROVINCE = lambda t: t['PROVINCE'].fillna(t['ADDRESS']).map(address.get_province,na_action='ignore'),
                    PAY_TYPE = lambda t: t['PAY_TYPE'].map(ViettelInfo.get_PAY_TYPE,na_action='ignore'),
                    PACKAGE_TYPE = lambda t: t['PACKAGE_TYPE'].map(lambda x: str(x).upper(),na_action='ignore'),
                    ACTIVE_DATE = lambda t: pd.to_datetime(t['ACTIVE_DATE'].map(text.remove_punctuation), format="%Y%m%d", errors= 'coerce'),
                    UPDATE_DATE = lambda t: pd.to_datetime(t['UPDATE_DATE'], format="%Y%m", errors= 'coerce'),
                    SUB_TYPE = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').typephone),
                    CARRIER = VinaphoneInfo.name,
                    IMPORT_MONTH = self.import_month,
                    SOURCE = "VENDOR_FTP_FILE",
                    )
            encrypt_df(data,'PHONE_NUMBER','IDCARD')
            return data.reindex(self.cols, axis = 1)
            
        except Exception as e:
            hp.cfg['log'].error(f"Fail to cleaning data {self.filename} from {data.index[0]} to {data.index[-1]} with error: {e}")
            # raise e
            return False
