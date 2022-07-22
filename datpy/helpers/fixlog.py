import pandas as pd
import re

class fixlog:
    """
    Phân tích file log, thống kê các error
    """

    def __init__(self, logpath):
        self.logpath = logpath

    def log_analysis(row):
        tex = row['detail']
        key_errortype = '>Fail to '
        key_errorvalue = 'value too large for column '
        condi = tex.startswith(key_errortype) and (key_errorvalue in tex)
        if condi:
            row['filename'] = tex[len(key_errortype):tex.index(' from')]
            pattern_index = re.findall("from [0-9]+ to [0-9]+",tex)[0]
            from_index = int(pattern_index[5:pattern_index.index(' to')])
            to_index = int(pattern_index[pattern_index.index(' to ')+4:])
            row['range_index_df'] = [from_index,to_index]
            error = tex[tex.index(key_errorvalue):]
            adjust = re.findall("actual: [0-9]+, maximum: [0-9]+",error)[0]
            row['COLUMN_ADDRESS'] = error[len(key_errorvalue):error.index(adjust)-2].replace("\"","")
            row['ACTUAL'] = re.findall("[0-9]+",adjust)[0]
            row['MAXIMUM_SETTING'] = re.findall("[0-9]+",adjust)[1]
        return row

    def fixlog_object(self):
        logdf = pd.read_csv(self.logpath,sep = "|",encoding='latin-1', names=['dt_status','name','filename','level','detail']).query("level == 'ERROR'")
        list_errortype = ', '.join(logdf.assign(detail_head = lambda t: t['detail'].str[:20]).detail_head.unique())
        print('Error types: ',list_errortype)
        logdf_ana = logdf.apply(fixlog.log_analysis,axis =1)
        print('Check out of range charactor column:\n',logdf_ana.groupby('COLUMN_ADDRESS',as_index=False)[['ACTUAL','MAXIMUM_SETTING']].max())
        dictprocess = logdf_ana.groupby('filename').agg({'range_index_df':lambda x: x.tolist()}).to_dict('index')
        dictprocess = {i:dictprocess[i]['range_index_df'] for i in dictprocess.keys()}
        return dictprocess
