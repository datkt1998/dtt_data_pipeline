
from datpy.helpers import helper as hp

# telco_info_mobifone
if __name__ == '__main__':
    dataname = 'telco_info'
    level2 = 'mobifone'
    tablename = 'DTTSD_TELCO_INFO'
    importMonth = '202107'
    schema = 'DTT_SD'
    hp.init()
    hp.load_params(dataname,tablename = tablename, level2 = level2, importMonth = importMonth, schema = schema)
    hp.run_main(hp.cfg)