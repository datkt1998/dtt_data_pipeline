
from datpy.helpers import helper as hp


if __name__ == '__main__':
    mode = 'telco_info_viettel'

    if mode == 'telco_info_mobifone':
        dataname = 'telco_info'
        level2 = 'mobifone'
        tablename = 'DTTSD_TELCO_INFO'
        importMonth = '202107'
        schema = 'DTT_SD'
        hp.init()
        hp.load_params(dataname,tablename = tablename, level2 = level2, importMonth = importMonth, schema = schema)
        hp.run_main(hp.cfg)

    elif mode == 'telco_info_viettel':
        dataname = 'telco_info'
        level2 = 'viettel'
        tablename = 'DTTSD_TELCO_INFO'
        importMonth = None
        schema = 'DTT_SD'
        hp.init()
        hp.load_params(dataname,tablename = tablename, level2 = level2, importMonth = importMonth, schema = schema)
        hp.run_main(hp.cfg,source = 'oracle')