#%%
import os
import importlib
import sys
from datetime import datetime
import datpy.database.connection as cn
import datpy.filetool.config_file as cf
import datpy.filetool.output_file as of
from datpy.helpers.fixlog import fixlog
from datpy.filetool.output_file import Mylog
from tqdm import tqdm
import time

def init():
    global cfg
    cfg = {}

def _path(*args):
    return os.path.normpath(os.path.join(*args))

def load_module(path, classname):
    folderpath = os.path.dirname(path)
    filename = os.path.basename(path).split('.')[0]
    sys.path.append(folderpath)
    res_class = getattr(importlib.import_module(filename), classname)
    return res_class

def setup_log():
    pass

def get_IMPORT_MONTH(importMonth):
    if importMonth is None:
        return None

    # if importMonth is None:
    #     return datetime.now().strftime('%Y%m')
    elif importMonth.endswith('.yaml'):
        return cf.Config(os.path.join(os.getcwd(),'configs',importMonth)).read(doc = 0, munch = False)['importMonth']
    else:
        return importMonth


def load_params(dataname,tablename = None, level2 = None, importMonth = None, schema = 'DTTSD'):
    level2 = level2.lower() if level2 is not None else level2
    importMonth =  get_IMPORT_MONTH(importMonth)
    RUNDIR = os.getcwd()
    CONFIG = cf.Config(os.path.join(RUNDIR,'configs','main_config.yaml')).read(doc = 0)
    folder_config = CONFIG.data[dataname].folder if level2 is None else CONFIG.data[dataname].folder[level2]
    tablename = (tablename if tablename is not None else list(dict(CONFIG.data[dataname].dataSchema[schema]).keys())[0]).upper()
    output_folder = _path(RUNDIR,folder_config.local_output)
    ftp_folder = folder_config.ftp_store
    savefolder,logfolder = of.OutputFolder(output_folder)
    dataSchema = dict(CONFIG.data[dataname].dataSchema[schema][tablename])
    run_class = load_module(folder_config.filecode, folder_config.classname)

    cfg['log'] = Mylog(logFolder = logfolder)()

    ftp_server = cn.FtpServer(CONFIG.database.db_source)
    oracle_server = cn.Database(CONFIG.database.db_target)

    cfg['schema'] = schema
    cfg['level2']=level2
    cfg['tablename']=tablename
    cfg['importMonth']=importMonth
    cfg['RUNDIR']=RUNDIR
    cfg['CONFIG']=CONFIG
    cfg['folder_config']=folder_config
    cfg['output_folder']=output_folder
    cfg['ftp_folder']=ftp_folder
    cfg['savefolder']=savefolder
    cfg['logfolder']=logfolder
    cfg['dataSchema']=dataSchema
    cfg['run_class']=run_class
    cfg['ftp_server']=ftp_server
    cfg['oracle_server']=oracle_server

    if folder_config.db_source is not None:
        source_oracle_server = cn.Database(folder_config.db_source)
        cfg['source_oracle_server'] = source_oracle_server
    

    cfg['log'].info(f'Get parameter: dataname = {dataname},tablename = {tablename}, level2 = {level2}, importMonth = {importMonth}, schema = {schema}')
    cfg['log'].info('Success to load parameter !')
    return cfg

# @cn.runtime
def run_main(cfg, run_downloadFile=True, source = 'ftp', mode = 'run_new', fixlog_filename = None):
    fixlog_dir = os.path.join(cfg['logfolder'],fixlog_filename) if fixlog_filename is not None else None
    fixlog_object = fixlog(fixlog_dir).fixlog_object() if fixlog_dir is not None else None
    
    if source == 'ftp':
        not_process_filefir = of.check_file_processed(cfg['ftp_server'].listdir(cfg['ftp_folder']),cfg['logfolder'])
        if fixlog_object is not None:
            not_process_filefir = [i for i in not_process_filefir if os.path.basename(i) in fixlog_object.keys()]
        for zipf in tqdm(not_process_filefir,desc = 'FTP file:', position = 0):
            download_file = cn.FtpFile(cfg['CONFIG'].database.db_source, zipf, cfg['savefolder'])
            download_file.process(run_download = run_downloadFile)
            unpacked_file = download_file.unpack_filelist
            for file in tqdm(unpacked_file, desc = 'Unpacked file:', position = 1, leave = False):
                if mode == 'run_new':
                    read_file = cfg['run_class'](file,cfg)
                    read_file.process(cfg['oracle_server'])
                    of.write_processedFile(zipf,cfg['logfolder'])
                elif (mode == 'fix_log') and (fixlog_object is not None) and (os.path.basename(file) in fixlog_object.keys()):
                    fixlog_ranges = fixlog_object[os.path.basename(file)]
                    read_file = cfg['run_class'](file,cfg, rangeIndex = fixlog_ranges)
                    read_file.process(cfg['oracle_server'])
            

    elif source == 'oracle':
        read_file = cfg['run_class'](filedir = None,cfg = cfg)
        read_file.process(cfg['oracle_server'])
        cf = cfg['folder_config'].db_source
        address_source = f"{cf.username}:{cf.password}@{cf.hostname}:{cf.port}/?service_name={cf.service_name}/{cf.tablename}"
        of.write_processedFile(address_source,cfg['logfolder'])

    os.startfile(cfg['logfolder'])



