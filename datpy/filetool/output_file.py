import os
import functools
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from datpy.helpers import helper as hp

class Mylog:
    """
    # logging.debug("This is a debug message")
    # logging.info("This is an informational message")
    # logging.warning("Careful! Something does not look right")
    # logging.error("You have encountered an error")
    # logging.critical("You are in trouble")
    """

    def __init__(self,logFolder=".",logLevel = logging.INFO, loggerName = None):
        self.folder = logFolder
        self.level = logLevel
        self.name = loggerName
        self.filedir = self.gen_LogFile()

    def gen_LogFile(self,):
        filename = 'logs_{}.log'.format(datetime.now().strftime("%Y_%m_%d_%H_%M"))
        return os.path.join(self.folder,filename)

    def get_logger(self, filedir = None):
        filedir = self.filedir if filedir is None else filedir
        log_format = "%(asctime)s|%(name)s|%(module)s(%(lineno)d)|%(levelname)s|>%(message)s"
        logging.basicConfig(filename=filedir, level=self.level, format=log_format,datefmt='%Y-%m-%d %H:%M:%S')
        return logging.getLogger(self.name)

    def __call__(self):
        return self.get_logger()


def logs(func=None,logger: Mylog = None, Success_message=None, Failure_message=None):
    """
    log apply to function
    """

    if not isinstance(logger, logging.RootLogger):
        logger = logger()
        assert isinstance(logger, logging.RootLogger)

    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            # appendInfo = appendInfo if type(appendInfo) == list else [appendInfo]
            signature = ", ".join(args_repr + kwargs_repr )
            try:
                result = func(*args, **kwargs)
                logger.info((f"Success in \'{func.__qualname__}\' with args {signature}") if Success_message is None else Success_message)
                return result
            except Exception as e:
                logger.error((f"Exception raised in \'{func.__qualname__}\' with args {signature} --> "+str(e)) if Failure_message is None else Failure_message)
                # raise e
        return wrapper
    if func is None:
        return decorator_log
    else:
        return decorator_log(func)



# lg = Mylog(r"D:\DATA SUPERLAKE\3. CODE\dtt_data_pipeline")()
# # lg.debug('this debug function2222')

# class sumHai:
#     def __init__(self,a,b):
#         self.a = a
#         self.b = b
    
#     @logs(logger = lg)
#     def sum(self,x = 3):
#         print(self.a / self.b+x)

# sumHai(1234,0).sum(x = 3)



def OutputFolder(mainFolder = os.getcwd()):
    save = os.path.join(mainFolder,'save')
    Path(save).mkdir(parents=True, exist_ok=True)
    logfolder = os.path.join(mainFolder,'logs')
    Path(logfolder).mkdir(parents=True, exist_ok=True)
    return save,logfolder

def check_file_processed(listdir,logFolder):
    processed_file = os.path.join(logFolder,'processed_filedir.csv')
    if os.path.exists(processed_file):
        processed_list = pd.read_csv(processed_file,sep="|")['filedir'].tolist()
        hp.cfg['log'].info("read file processed_filedir.csv")
    else:
        processed_list=[]
        hp.cfg['log'].warning("not found processed_filedir.csv")
    not_processed_list = [i for i in listdir if i not in processed_list]
    not_processed_file = os.path.join(logFolder,'not_processed_filedir.csv')
    pd.DataFrame({'not_process_file':not_processed_list}).to_csv(not_processed_file,sep="|",index=False)
    hp.cfg['log'].info("exported not_processed_filedir.csv")
    print('Count files will process:', len(not_processed_list))
    return not_processed_list

def write_processedFile(filedir = None,logFolder = None):
    df = pd.DataFrame({'filedir':filedir,'process_datetime':str(datetime.now())},index=[0])
    output_file = os.path.join(logFolder,'processed_filedir.csv')
    df.to_csv(output_file,index=False,sep="|", mode='a', header=not os.path.exists(output_file))
    hp.cfg['log'].info(f"Append {filedir} to processed_filedir.csv")






