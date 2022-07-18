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
        self.name = cfg['name_level2'].title()
        self.rangeIndex = rangeIndex
        self.schema = cfg['schema']
        self.tablename = cfg['tablename']
        self.dataSchema = cfg['dataSchema']
        self.cols = self.dataSchema.keys()
        self.chunksize = 10000
        self.filename = os.path.basename(filedir)
        self.datacols = sorted(pd.read_csv(filedir, on_bad_lines='skip',sep = ",", nrows= 1).columns.tolist())
        self.datachunk = pd.read_csv(filedir, on_bad_lines='skip',sep = ",", chunksize =self.chunksize)
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
        self.import_month, self.pay_type = importMonthInfo(cfg['ftp_filedir'],'telco_info_viettel')
        self.validate_inputdata()
        self.quarter = "Q{}".format((int(self.import_month[-2:])-1) // 3 +1)
        self.tablename = self.tablename + f"_{self.import_month[:-2]}{self.quarter}"

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

