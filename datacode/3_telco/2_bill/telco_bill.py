from datpy.vmg.cleaning import text, address, Phone,Person
from datpy.vmg.codecrypt import encrypt_df, decrypt_df
import pandas as pd
import os
from datpy.helpers import helper as hp
import numpy as np
from datpy.helpers.clean_job import CleanProcess, importMonthInfo

class ViettelBill_ftp(CleanProcess):
    source = "VENDOR_FTP_FILE"
    def __init__(self,filedir,cfg,rangeIndex=None):
        self.filedir = filedir
        self.cfg = cfg
        self.name = cfg['name_level2'].title()
        self.rangeIndex = rangeIndex
        self.schema = cfg['schema']
        self.tablename = cfg['tablename']
        self.dataSchema = cfg['dataSchema']
        self.cols = self.dataSchema.keys()
        self.chunksize = 10000
        self.sep = ","
        self.rename_dict = {'so_dien_thoai':"PHONE_NUMBER",
                            'quan_huyen':"DISTRICT",
                            'tinh_thanh_pho':"PROVINCE",
                            'so_giay_goi':"LL_CALL",
                            'so_tin_nhan':"LL_SMS",
                            'tien_cuoc_goi':"BILL_CALL",
                            'tien_cuoc_tin_nhan':"BILL_SMS",
                            'tien_cam_ket_va_vas':"BILL_VAS",
                            'tien_vas':"BILL_VAS",
                            'dong_may':"PHONE_TYPE",
                            'nam_sinh':"BIRTHYEAR",
                            'gioi_tinh':"GENDER",
                            'ngay_kich_hoat':"ACTIVE_DATE",
                            'tong_cuoc_goc':"BILL_TOTAL",
                            'tong_tieu_dung':"BILL_TOTAL",
                            'ngay_sinh':"DOB"}
        self.import_month, self.pay_type = importMonthInfo(cfg['ftp_filedir'],'telco_bill_viettel')
        self.quarter = "Q{}".format((int(self.import_month[-2:])-1) // 3 +1)
        self.tablename = self.tablename + f"_{self.import_month[:-2]}{self.quarter}"
        self.filename, self.datacols, self.datachunk = self.getDataReader()
        self.validate_inputdata()

    def cleaning_pandas(self,data):
        try:
            data.columns = [i.lower() for i in data.columns]
            data = data\
                .rename(columns=self.rename_dict)\
                .applymap(lambda x: text(x).clean())\
                .assign(
                    PHONE_NUMBER = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').cleaned),
                    GENDER = lambda t: t['GENDER'].map(Person.gender_standardize),
                    PROVINCE = lambda t: t['PROVINCE'].map(address.get_province,na_action='ignore'),
                    PHONE_TYPE = lambda t: t['PHONE_TYPE'].map(str.title,na_action='ignore'),
                    PAY_TYPE = Phone.pay_type(self.pay_type),
                    DOB = lambda t: pd.to_datetime(t['DOB'], format="%Y/%m/%d", errors= 'coerce') if 'DOB' in t.columns else np.datetime64('NaT'),
                    BIRTHYEAR = lambda t: t['BIRTHYEAR'].map(Person.birthyear_standardize ,na_action='ignore') if 'BIRTHYEAR' in t.columns else np.nan,
                    ACTIVE_DATE = lambda t: pd.to_datetime(t['ACTIVE_DATE'], format="%Y%m%d", errors= 'coerce'),
                    CARRIER =self.name,
                    ISSUE_MONTH = self.import_month,
                    SOURCE = ViettelBill_ftp.source)
            data[["LL_CALL","LL_SMS"]] = data[["LL_CALL","LL_SMS"]].fillna(0).astype(int)
            data[["BILL_CALL","BILL_SMS",'BILL_VAS','BILL_TOTAL']] = data[["BILL_CALL","BILL_SMS",'BILL_VAS','BILL_TOTAL']].astype(float)
            encrypt_df(data,'PHONE_NUMBER')
            return data.reindex(self.cols, axis = 1)
        except Exception as e:
            hp.cfg['log'].error(f"Fail to cleaning data {self.filename} from {data.index[0]} to {data.index[-1]} with error: {e}")
            return False





class VinaphoneBill_ftp(CleanProcess):

    def __init__(self,filedir,cfg,rangeIndex=None):
        self.source = "VENDOR_FTP_FILE"
        self.filedir = filedir
        self.cfg = cfg
        self.name = 'Vinaphone'
        self.rangeIndex = rangeIndex
        self.schema = cfg['schema']
        self.tablename = cfg['tablename']
        self.dataSchema = cfg['dataSchema']
        self.cols = self.dataSchema.keys()
        self.chunksize = 10000
        self.sep = "|"
        self.rename_dict = {'SO_THUE_BAO':"PHONE_NUMBER",'ISDN':"PHONE_NUMBER",
                            'NGAY_SINH':"DOB",
                            'LOAI_TB': 'PAY_TYPE',
                            'DIA_CHI' : 'ADDRESS',
                            'LL_DATA' : 'LL_DATA',
                            'LL_SMS': 'LL_SMS',
                            'LL_THOAI': 'LL_THOAI',
                            'TKC_THOAI':"BILL_CALL",'TOT_VOICE':"BILL_CALL",
                            'TKC_DATA':"BILL_DATA",'TOT_DATA':"BILL_DATA",
                            'TKC_SMS':"BILL_SMS",'TOT_SMS':"BILL_SMS",
                            'TKC_VAS':"BILL_VAS",'TOT_VAS':"BILL_VAS",
                            'DONG_MAY':"PHONE_BRAND",
                            'LOAI_MAY':"PHONE_TYPE",
                            'GIOI_TINH':"GENDER",
                            'NGAY_KICH_HOAT':"ACTIVE_DATE",'NGAY_ACTIVE':"ACTIVE_DATE",
                            'GOI':"PACKAGE_TYPE",'GOI_DATA': 'DATA_PACKAGE_TYPE',
                            'TKC':'BILL_TOTAL','TOT_TKC':"BILL_TOTAL",
                            'SO_DU_TKC':'MAIN_BALANCE'}
        self.import_month= importMonthInfo(cfg['ftp_filedir'],'telco_bill_vinaphone')
        self.quarter = "Q{}".format((int(self.import_month[-2:])-1) // 3 +1)
        self.tablename = self.tablename + f"_{self.import_month[:-2]}{self.quarter}"
        self.filename, self.datacols, self.datachunk = self.getDataReader()
        self.validate_inputdata()

    def cleaning_pandas(self,data):
        try:
            data.columns = [i.upper() for i in data.columns]
            data = data\
                .rename(columns=self.rename_dict)\
                .applymap(lambda x: text(x).clean())\
                .assign(
                    PHONE_NUMBER = lambda t: t['PHONE_NUMBER'].map(lambda x: Phone(x,error = 'ignore').cleaned),
                    PHONE_BRAND = lambda t: t['PHONE_BRAND'].map(str.title,na_action='ignore'),
                    GENDER = lambda t: t['GENDER'].map(Person.gender_standardize),
                    ADDRESS = lambda t: t['ADDRESS'].map(lambda x: str(x).title(),na_action='ignore'),
                    PROVINCE = lambda t: t['ADDRESS'].map(address.get_province,na_action='ignore'),
                    PHONE_TYPE = lambda t: t['PHONE_TYPE'].map(str.title,na_action='ignore'),
                    PAY_TYPE = lambda t: t['PHONE_TYPE'].map(Phone.pay_type),
                    ACTIVE_DATE = lambda t: pd.to_datetime(t['ACTIVE_DATE'], dayfirst=True, errors= 'coerce'),
                    DOB = lambda t: pd.to_datetime(t['DOB'], dayfirst=True, errors= 'coerce'),
                    CARRIER =self.name,
                    ISSUE_MONTH = self.import_month,
                    SOURCE = self.source)
            data[[i for i in data.columns if i.startswith("LL")]] = data[[i for i in data.columns if i.startswith("LL")]].fillna(0).astype(float)
            data[[i for i in data.columns if i.startswith("BILL")]] = data[[i for i in data.columns if i.startswith("BILL_")]].astype(float)
            encrypt_df(data,'PHONE_NUMBER')
        # raise
            return data.reindex(self.cols, axis = 1)
        except Exception as e:
            hp.cfg['log'].error(f"Fail to cleaning data {self.filename} from {data.index[0]} to {data.index[-1]} with error: {e}")
            return False

