import argparse
from datpy.helpers import helper as hp

def parse_args():
    parser = argparse.ArgumentParser(description="Run mode 'run_new' or 'fix_log'")
    parser.add_argument('--mode', nargs='?', default='run_new') # ['run_new','fix_log']
    parser = argparse.ArgumentParser(description="Choose the config to run")
    parser.add_argument('--cfg', nargs='?', default=None)
    parser = argparse.ArgumentParser(description="if choose the mode fix_log, the 'logname' parameter is the log filename to check for errors")
    parser.add_argument('--logname', nargs='?', default=None)
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    # cfg = args.cfg
    # mode = args.mode
    # fixlog_filename = args.logname
    cfg = 'telco_info_vinaphone'
    mode = 'run_new'
    fixlog_filename = 'logs_2022_07_11_11_56.log'

    if cfg == 'telco_info_mobifone':
        dataname = 'telco_info'
        level2 = 'mobifone'
        tablename = 'DTTSD_TELCO_INFO'
        importMonth = '202107'
        schema = 'DTT_SD'
        hp.init()
        hp.load_params(dataname,tablename = tablename, level2 = level2, importMonth = importMonth, schema = schema)
        hp.run_main(hp.cfg,source = 'ftp', mode = mode, fixlog_filename=fixlog_filename)

    elif cfg == 'telco_info_viettel':
        dataname = 'telco_info'
        level2 = 'viettel'
        tablename = 'DTTSD_TELCO_INFO'
        importMonth = None
        schema = 'DTT_SD'
        hp.init()
        hp.load_params(dataname,tablename = tablename, level2 = level2, importMonth = importMonth, schema = schema)
        hp.run_main(hp.cfg,source = 'oracle', mode = mode, fixlog_filename=fixlog_filename)

    elif cfg == 'telco_info_vinaphone':
        dataname = 'telco_info'
        level2 = 'vinaphone'
        tablename = 'DTTSD_TELCO_INFO'
        importMonth = 'telco_info_vinaphone.yaml'
        schema = 'DTT_SD'
        hp.init()
        hp.load_params(dataname,tablename = tablename, level2 = level2, importMonth = importMonth, schema = schema)
        hp.run_main(hp.cfg,source = 'ftp', mode = mode, fixlog_filename=None, run_downloadFile =False)