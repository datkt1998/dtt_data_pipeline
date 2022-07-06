#%%
import json
import requests
import numpy as np
from unidecode import unidecode
from string import punctuation
import re 
import tqdm
from difflib import get_close_matches
import pandas as pd

class address:

    list_province = ['An Giang', 'Bà Rịa - Vũng Tàu', 'Bạc Liêu', 'Bắc Giang', 
    'Bắc Kạn', 'Bắc Ninh', 'Bến Tre', 'Bình Dương', 'Bình Định', 'Bình Phước', 
    'Bình Thuận', 'Cà Mau', 'Cao Bằng', 'Cần Thơ', 'Đà Nẵng', 'Đắk Lắk', 
    'Đắk Nông', 'Điện Biên', 'Đồng Nai', 'Đồng Tháp', 'Gia Lai', 'Hà Giang', 
    'Hà Nam', 'Hà Nội', 'Hà Tĩnh', 'Hải Dương', 'Hải Phòng', 'Hậu Giang', 
    'Hòa Bình', 'Hưng Yên', 'Khánh Hòa', 'Kiên Giang', 'Kon Tum', 'Lai Châu', 
    'Lạng Sơn', 'Lào Cai', 'Lâm Đồng', 'Long An', 'Nam Định', 'Nghệ An', 
    'Ninh Bình', 'Ninh Thuận', 'Phú Thọ', 'Phú Yên', 'Quảng Bình', 'Quảng Nam', 
    'Quảng Ngãi', 'Quảng Ninh', 'Quảng Trị', 'Sóc Trăng', 'Sơn La', 'Tây Ninh', 
    'Thái Bình', 'Thái Nguyên', 'Thanh Hóa', 'Thành phố Hồ Chí Minh', 
    'Thừa Thiên Huế', 'Tiền Giang', 'Trà Vinh', 'Tuyên Quang', 'Vĩnh Long', 
    'Vĩnh Phúc', 'Yên Bái']
    unidecode_province = {unidecode(i).lower():i for i in list_province}
    unidecode_province.update({unidecode(i).lower().replace(' ',""):i for i in list_province})
    unidecode_province.update({'brvt':'Bà Rịa - Vũng Tàu',
        "hn":'Hà Nội','tphn':'Hà Nội','ho chi minh':'Thành phố Hồ Chí Minh',
        'hcm': 'Thành phố Hồ Chí Minh', 'ba ria vung tau': 'Bà Rịa - Vũng Tàu',
        'ba ria': 'Bà Rịa - Vũng Tàu', 'vung tau': 'Bà Rịa - Vũng Tàu','hue':'Thừa Thiên Huế',
        'daklak':'Đắk Lắk', 'daknong':'Đắk Nông', 'tphcm': 'Thành phố Hồ Chí Minh',
        'bac can': 'Bắc Kạn', 'ha tay':'Hà Nội','kontum': 'Kon Tum',
    })
    listkey = list(unidecode_province.keys())

    def __init__(self,):
        pass

    def splitAdress(
        data, 
        key_return=[ "ward_short", "district_short","province_short"], 
        max_data=500,
        if_error= None #VNAddressStandardizer
    ):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        res = None

        def return_if_error(x,if_error):
            return if_error(x, comma_handle = False).execute() if if_error is not None else None
        
        def split_1_address(data : str):
                url = "http://192.168.45.45:8030/single_address"
                try:
                    r = requests.post(url, data=json.dumps({"address": data}), headers=headers)
                    res = [json.loads(r.text)["address"][i].title() for i in key_return]
                    assert res != [""]*len(key_return)
                except:
                    try:
                        data = return_if_error(data,if_error)
                        r = requests.post(url, data=json.dumps({"address": data}), headers=headers)
                        res = [json.loads(r.text)["address"][i].title() for i in key_return]
                    except:
                        res=[""]*len(key_return)
                return res
            
        def split_list_address(data : list):
            url = "http://192.168.45.45:8030/address_list"
            sublist_data = [data[x : x + max_data] for x in range(0, len(data), max_data)]
            res = []
            # total_amount = len(data)
            amount=0
            for sublist in sublist_data:
                amount += max_data
                try:
                    response = requests.post(url, data=json.dumps({"address_list": sublist}), headers=headers).json()
                    response_key_return=[[a["address"][i].title()  for i in key_return] if (a["address"] !="") else ([""]*len(key_return)) for a in response]
                except:
                    response_key_return=[split_1_address(add1) for add1 in sublist]
                res += response_key_return
                print("",end="\r")
                # print("Done {}/{}".format(min(amount,total_amount),total_amount),end="")
            return res
        
        try:
            if (data != data) or (data==""):
                return None
            if type(data) == str:
                res = split_1_address(data)
            elif type(data) == list:
                res = split_list_address(data)
            else:
                print("Sai type of data !")
            return res
        except:
            raise
    
    def standardize(data):
        res = address.splitAdress(data)
        if type(data) == list:
            return list(map(lambda x: ", ".join(x),res))
        if type(data) == str:
            return ", ".join(res)

    def get_province(address_data:str):

        if address_data != address_data:
            return np.nan

        def rfind_nth(string, substring=" ", n=1, max_num_char = 3):
            end = string.rfind(substring)
            while end >= 0 and n > 1:
                end = string.rfind(substring,0, end)
                n -= 1
            return " ".join(string[end+1:].split(" ")[:max_num_char])

        add_replacepunc = address_data.translate(str.maketrans(punctuation, ' '*len(punctuation))) #map punctuation to space
        add_clean = unidecode(add_replacepunc).replace("  "," ").strip().lower()
        count_word_use = 0
        while count_word_use < 10:
            count_word_use+=1
            check = rfind_nth(add_clean,n = count_word_use)
            for minscore in [1, 0.99, 0.95, 0.9, 0.85, 0.8]:
                res = get_close_matches(check,address.listkey, n = 1, cutoff=minscore)
                if len(res)>0:
                    return address.unidecode_province[res[0]]  
        return address.splitAdress(data = address_data, key_return = ["province_short"])[0]


    # Phan tich Address
    def address_compare_score(A, B):
        A_split = np.array([unidecode(x) for x in address.splitAdress(A)])
        B_split = np.array([unidecode(x) for x in address.splitAdress(B)])
        score = np.cumprod(A_split == B_split).sum()
        return score

    
class text:

    def __init__(self,value):
        self.value = str(value)
    
    def encrypt(self):
        url = "http://192.168.45.45:8779/en"
        res = requests.post(url, data=self.value).text
        return res

    def compare_name(self,withName):
        name1 = self.value
        name2 = withName
        if type(name1) != str or type(name2) != str:
            return None
        if name1.isnumeric() or name2.isnumeric():
            return None
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            url = 'http://192.168.45.45:8030/compare_names'
            pair = json.dumps({"name1" : name1, "name2" : name2})
            x = requests.post(url, data = pair,headers=headers)
            res = json.loads(x.text)['match']
            return res

    def remove_punctuation(str_data):
        str_data = str_data.strip()
        for e in ['.0','.00',',0',',00']:
            if str_data.endswith(e):
                str_data = str_data[:-len(e)]
                break
        str_data = str_data.translate(str.maketrans('', '', punctuation))
        return str_data

    def clean(self):
        x = self.value
        if type(x)==str:
            list_nan_encrypt =[
            "NqH2T0OqGhYeJKBEFFzNmg==","WT0GPRKcQxiTXjDpOncVhw==","8zeFSJ6JoUFhmf9cA1NRDg==",
            "JTejTlFoNwzfCBJMcR0HiQ==","V+0CZRxDddEVQw2rxwmiwA==","lVKQ/5YMX3x+hOdbtW0F1w==",
            "Yh9tPt/GcByge8grvB8R/w==",
        ]
            x=x.strip()
            condi = (x.lower() in ["","null","nan","#n/a","#ref!",'none','.',',','n/a','<na>']) or (x in list_nan_encrypt)
            if condi :
                return np.nan
            else:
                return x

        elif (x is np.nan) or (x is None): # None or nan
            return np.nan

        elif type(x) == list:
            return [text(i).clean() for i in x]

        else:
            return x
        



class IDcard(text):

    """ 
    Đánh giá một chuỗi có phải là căn cước công dân hợp lệ hay không
    Các giá trị trả về:
    Type        Ý nghĩa                             Mô tả cách phân loại
    1           CMND                                9 ký tự số hợp lệ
    2           CMND 9 số có tỉnh                   9 ký tự số + 3 ký tự chữ ở cuối
    3           CCCD/CMND 12 số                     12 ký tự số hợp lệ
    4           Hộ chiếu VN                         Ký tự chữ ở đầu + 7 ký tự số ở cuối
    5           Hộ chiếu nước ngoài                 Có 8/9 ký tự, bao gồm cả ký tự chữ và số
    6           Nghi ngờ CMND mất 0                 8 ký tự số, khi thêm 0 được CMND hợp lệ
    7           Nghi ngờ CCCD/CMND 12 số mất 0      11 ký tự số, khi thêm 0 được CCCD/CMND 12 số hợp lệ
    8           Không hợp lệ                        Các trường hợp còn lại
    """

    CCCDrule = {
    "001": ["Hà Nội"],
    "002": ["Hà Giang"],
    "004": ["Cao Bằng"],
    "006": ["Bắc Kạn"],
    "008": ["Tuyên Quang"],
    "010": ["Lào Cai"],
    "011": ["Điện Biên"],
    "012": ["Lai Châu"],
    "014": ["Sơn La"],
    "015": ["Yên Bái"],
    "017": ["Hòa Bình"],
    "019": ["Thái Nguyên"],
    "020": ["Lạng Sơn"],
    "022": ["Quảng Ninh"],
    "024": ["Bắc Giang"],
    "025": ["Phú Thọ"],
    "026": ["Vĩnh Phúc"],
    "027": ["Bắc Ninh"],
    "030": ["Hải Dương"],
    "031": ["Hải Phòng"],
    "033": ["Hưng Yên"],
    "034": ["Thái Bình"],
    "035": ["Hà Nam"],
    "036": ["Nam Định"],
    "037": ["Ninh Bình"],
    "038": ["Thanh Hóa"],
    "040": ["Nghệ An"],
    "042": ["Hà Tĩnh"],
    "044": ["Quảng Bình"],
    "045": ["Quảng Trị"],
    "046": ["Thừa Thiên Huế"],
    "048": ["Đà Nẵng"],
    "049": ["Quảng Nam"],
    "051": ["Quảng Ngãi"],
    "052": ["Bình Định"],
    "054": ["Phú Yên"],
    "056": ["Khánh Hòa"],
    "058": ["Ninh Thuận"],
    "060": ["Bình Thuận"],
    "062": ["Kon Tum"],
    "064": ["Gia Lai"],
    "066": ["Đắk Lắk"],
    "067": ["Đắk Nông"],
    "068": ["Lâm Đồng"],
    "070": ["Bình Phước"],
    "072": ["Tây Ninh"],
    "074": ["Bình Dương"],
    "075": ["Đồng Nai"],
    "077": ["Bà Rịa - Vũng Tàu"],
    "079": ["Hồ Chí Minh"],
    "080": ["Long An"],
    "082": ["Tiền Giang"],
    "083": ["Bến Tre"],
    "084": ["Trà Vinh"],
    "086": ["Vĩnh Long"],
    "087": ["Đồng Tháp"],
    "089": ["An Giang"],
    "091": ["Kiên Giang"],
    "092": ["Cần Thơ"],
    "093": ["Hậu Giang"],
    "094": ["Sóc Trăng"],
    "095": ["Bạc Liêu"],
    "096": ["Cà Mau"],
        }
    # Chứng minh nhân dân 9 số
    CMNDrule = {
    "01": ["Hà Nội"],
    "02": ["Hồ Chí Minh"],
    "03": ["Hải Phòng"],
    "04": ["Điện Biên", "Lai Châu"],
    "05": ["Sơn La"],
    "06": ["Lào Cai", "Yên Bái"],
    "07": ["Hà Giang", "Tuyên Quang"],
    "08": ["Lạng Sơn", "Cao Bằng"],
    "090": ["Thái Nguyên"],
    "091": ["Thái Nguyên"],
    "092": ["Thái Nguyên"],
    "095": ["Bắc Kạn"],
    "10": ["Quảng Ninh"],
    "11": ["Hà Tây", "Hòa Bình", "Hà Nội"],
    "12": ["Bắc Giang", "Bắc Ninh"],
    "13": ["Phú Thọ", "Vĩnh Phúc"],
    "14": ["Hải Dương", "Hưng Yên"],
    "15": ["Thái Bình"],
    "16": ["Nam Định", "Hà Nam", "Ninh Bình"],
    "17": ["Thanh Hóa"],
    "18": ["Nghệ An", "Hà Tĩnh"],
    "19": ["Quảng Bình", "Quảng Trị", "Thừa Thiên - Huế"],
    "20": ["Quảng Nam", "Đà Nẵng"],
    "21": ["Quảng Ngãi", "Bình Định"],
    "22": ["Khánh Hòa", "Phú Yên"],
    "230": ["Gia Lai"],
    "231": ["Gia Lai"],
    "23": ["Kon Tum"],
    "24": ["Đắk Lắk"],
    "245": ["Đắk Nông"],
    "25": ["Lâm Đồng"],
    "26": ["Ninh Thuận", "Bình Thuận"],
    "27": ["Đồng Nai", "Bà Rịa - Vũng Tàu"],
    "280": ["Bình Dương"],
    "281": ["Bình Dương"],
    "285": ["Bình Phước"],
    "29": ["Tây Ninh"],
    "30": ["Long An"],
    "31": ["Tiền Giang"],
    "32": ["Bến Tre"],
    "33": ["Vĩnh Long", "Trà Vinh"],
    "34": ["Đồng Tháp"],
    "35": ["An Giang"],
    "36": ["Cần Thơ", "Hậu Giang", "Sóc Trăng"],
    "37": ["Kiên Giang"],
    "38": ["Cà Mau", "Bạc Liêu"],
        }
    typeError = {
    '1': "CMND", 
    '2': "CMND 9 số có tỉnh", 
    '3': "CCCD/CMND 12 số", 
    "4": "Hộ chiếu VN", 
    "5": "Hộ chiếu nước ngoài",
    "6": "Nghi ngờ CMND mất 0", 
    "7": "Nghi ngờ CCCD/CMND 12 số mất 0",
    "8": "Không hợp lệ"
        }
    typeStandard = {
    "1": "PASSPORT_VN", 
    "2": "PASSPORT_NN",
    "3": "CMND_9", 
    "4": "CCCD_12",
    "5": "unknown"
        }

    def __init__(self,value):
        super().__init__(value)

    def cleankey(self,error = 'ignore') :
        value = self.clean()
        if type(value) == str:
            try:

                cleaned = text.remove_punctuation(self.clean()).replace(" ","")
                runtime = 0
                while (len([s for s in cleaned if s.isdigit()]) > 12) and (runtime <30):
                    runtime+=1
                    if cleaned[0] == '0':
                        cleaned = cleaned[1:]
                    else:
                        continue

                if runtime == 30:
                    raise

                res = cleaned
            except:
                if error == 'ignore':
                    res = self.value
                if error == 'nan':
                    res = np.nan
            return res
        elif type(value) == list:
            return [IDcard(i).cleankey(error = error) for i in value]

    def typeIDcode(self,):
        def isAllNumber(text):
            text = str(text)
            for char in text:
                if not char.isnumeric():
                    return False
            return True 

        def isAllAlpha(text):
            text = str(text)
            for char in text:
                if not char.isalpha():
                    return False 
            return True 

        def is9IDCard(text):
            if text[0:2] in IDcard.CMNDrule.keys() or text[0:3] in IDcard.CMNDrule.keys():
                return True
            else:
                return False

        def is12IDCard(text):
            if text[0:3] not in IDcard.CCCDrule.keys():
                # Sai ma tinh 
                return False 
            elif int(text[3]) not in range(10): # ?
                # print("Sai gioi tinh")
                return False
            elif int(text[3]) > 1 and int(text[4:6]) > 25:
                # sinh năm >= 2000 và năm sinh >= 2025
                # print("Sai nam sinh")
                return False
            return True 
        
        def get_codetypeID(text):
            if isAllAlpha(text):
                # Chỉ chứa ký tự chữ 
                return "8"
            if len(text) == 12:
                if isAllNumber(text):
                    if is12IDCard(text):
                    # "CCCD/CMND 12 số: 12 ký tự số hợp lệ",
                        return '3'
                    else:
                        return '8'
                elif isAllNumber(text[0:9]) and is9IDCard(text[0:9]) and isAllAlpha(text[9:12]):
                    # CMND 9 số có tỉnh: 9 ký tự số + 3 ký tự chữ ở cuối
                    return '2'
                else:
                    return '8'

            elif len(text) == 11 and isAllNumber(text):
                if is12IDCard("0"+text) :
                # Nghi ngờ CCCD/CMND 12 số mất 0: 11 ký tự số, khi thêm 0 được CCCD/CMND 12 số hợp lệ
                    return '7'
                else:
                    return '8'
            elif len(text) == 10 and isAllNumber(text):
                if is12IDCard("00"+text) :
                # Nghi ngờ CCCD/CMND 12 số mất 0: 11 ký tự số, khi thêm 0 được CCCD/CMND 12 số hợp lệ
                    return '7'
                else:
                    return '8'

            elif len(text) == 8:
                if re.match("[a-zA-Z][0-9]{7}", text):
                    # Hộ chiếu VN: Ký tự chữ ở đầu + 7 ký tự số ở cuối
                    return '4'
                elif isAllNumber(text): 
                    if is9IDCard("0" + text) :
                    # Nghi ngờ CMND mất 0: 8 ký tự số, khi thêm 0 được CMND hợp lệ
                        return '6'
                    else:
                        # 8 ký tự số, vẫn sai khi thêm 0
                        return "8"#"6a"
                elif re.match("[a-zA-Z0-9]{8}", text):
                    return "5"#"5a"
                else:
                    return '8'

            elif len(text) == 9:
                if isAllNumber(text):
                    if is9IDCard(text):
                    # CMND: 9 ký tự số hợp lệ
                        return '1'
                    else:
                        # 9 ký tự số, sai mã tỉnh 
                        return "8"#"1a" 
                elif re.match("[a-zA-Z0-9]{9}", text):
                    return "5"#"5b"
                else:
                    return '8'

            else:
                return '8'

        return get_codetypeID(self.cleankey())
    
    def typeIDraw(self):
        return IDcard.typeError[self.typeIDcode()]

    def typeIDstandard(self,error = 'ignore'):
        cleaned = self.cleankey(error=error)
        typeID = self.typeIDcode()
        if ((typeID =='3') and (len(cleaned) == 12)) or (typeID =='7'):
            return IDcard.typeStandard['4']
        elif (typeID in ['1','2','6']) or ((typeID =='3') and (len(cleaned) == 9)):
            return IDcard.typeStandard['3']
        elif ( typeID =='4' ):
            return IDcard.typeStandard['1']
        elif ( typeID =='5' ):
            return IDcard.typeStandard['2']
        else:
            return IDcard.typeStandard['5']

    def standardize(self,error = 'ignore', encrypt = False):
        cleaned = self.cleankey(error=error)
        cleaned = cleaned.upper() if type(cleaned) == str else cleaned
        typeID = self.typeIDcode()
        if (typeID in ['1','3','4','5']):
            res = cleaned
        elif (typeID == '2'):
            res = cleaned[:-3]
        elif (typeID == '6'):
            res = "{:09.0f}".format(int(cleaned))
        elif (typeID == '7'):
            res = "{:012.0f}".format(int(cleaned))
        else:
            res = np.nan

        if encrypt:
            res = text(res).encrypt()

        return res

class Phone(text):

    def __init__(self,value = None, dialcode = "84", error = 'ignore', encrypt = False):
        super().__init__(value)
        self.dialcode = dialcode
        self.error = error
        self.is_encrypt = encrypt
        self.chuyendausodidong = [
            ['162','32'],['163','33'],['164','34'],['165','35'],['166','36'],['167','37'],['168','38'],['169','39'],
            ['120','70'],['121','79'],['122','77'],['126','76'],['128','78'],['123','83'],['124','84'],['125','85'],
            ['127','81'],['129','82'],['182','52'],['186','56'],['188','58'],['199','59']
            ]
        self.chuyendausocodinh = [
            ['76', '296'], ['64', '254'], ['281', '209'], ['240', '204'], ['781', '291'], ['241', '222'], ['75', '275'],
            ['56', '256'], ['650', '274'], ['651', '271'], ['62', '252'], ['780', '290'], ['710', '292'], ['26', '206'],
            ['511', '236'], ['500', '262'], ['501', '261'], ['230', '215'], ['61', '251'], ['67', '277'], ['59', '269'],
            ['219', '219'], ['351', '226'], ['4', '24'], ['39', '239'], ['320', '220'], ['31', '225'], ['711', '293'],
            ['8', '28'], ['218', '218'], ['321', '221'], ['8', '258'], ['77', '297'], ['60', '260'], ['231', '213'],
            ['63', '263'], ['25', '205'], ['20', '214'], ['72', '272'], ['350', '228'], ['38', '238'], ['30', '229'],
            ['68', '259'], ['210', '210'], ['57', '257'], ['52', '232'], ['510', '235'], ['55', '255'], ['33', '203'],
            ['53', '233'], ['79', '299'], ['22', '212'], ['66', '276'], ['36', '227'], ['280', '208'], ['37', '237'],
            ['54', '234'], ['73', '273'], ['74', '294'], ['27', '207'], ['70', '270'], ['211', '211'], ['29', '216'],
        ]
        self.dausodidong = [i[1] for i in self.chuyendausodidong] + \
                            ['86','96','97','98','88','91','94','89','90','93','92','56','58','99','87']
        self.dausocodinh = [i[1] for i in self.chuyendausocodinh]
        self.typephone = 'unknown'
        self.cleaned = self.standardize()

    def cleankey(self):
        dialcode = self.dialcode
        def isAllNumber(text):
            for char in text:
                if not char.isnumeric():
                    return False
            return True 
        cleaned = text.remove_punctuation(self.clean()).replace(" ","")
        assert isAllNumber(cleaned)

        runtime = 0

        while (len(cleaned) >= 9) and (runtime<30) and (self.typephone == 'unknown'):

            cleaned = cleaned[1:] if cleaned[0] == '0' else cleaned
            if ((cleaned[:len(dialcode)] == dialcode) and len(cleaned)>=(9+len(dialcode))):
                cleaned = cleaned[len(dialcode):]

            if (len(cleaned) == 9) and (self.typephone == 'unknown'):
                for i in self.dausodidong:
                    if cleaned.startswith(i):
                        self.typephone = 'so_di_dong'
                        break

            if (len(cleaned) in [9,10]) and (self.typephone == 'unknown'):
                for i in self.dausocodinh:
                    if cleaned.startswith(i) and (len(cleaned[len(i):]) in [7,8]): # ví du 0220 3736 596 (HD), 024 392 63087 (HN)
                        self.typephone = 'so_co_dinh'
                        break

            if (len(cleaned) == 10) and (self.typephone == 'unknown'):
                for pair in self.chuyendausodidong:
                    if cleaned.startswith(pair[0]):
                        cleaned = cleaned.replace(pair[0],pair[1],1)
                        self.typephone = 'so_di_dong'
                        break

            if (len(cleaned) in [9,10]) and (self.typephone == 'unknown'):
                for pair in self.chuyendausocodinh:
                    if cleaned.startswith(pair[0]) and (len(cleaned[len(pair[0]):]) in [7,8]): # ví du 0220 3736 596 (HD), 024 392 63087 (HN)
                        cleaned = cleaned.replace(pair[0],pair[1],1)
                        self.typephone = 'so_co_dinh'
                        break

            runtime+=1

        if runtime == 30:
            raise ValueError
        else:
            return cleaned

    def standardize(self):
        try:
            res = self.dialcode + self.cleankey()
        except:
            if self.error == 'ignore':
                res = self.value
            if self.error == 'nan':
                res =  np.nan
        if self.is_encrypt:
            res = text(res).encrypt()
        return res

#%%
# Phone('01234208162').standardize()
#%%

class SI(text):
    def __init__(self,value):
        super().__init__(value)

    def cleankey(self):

        def isAllNumber(text):
            for char in text:
                if not char.isnumeric():
                    return False
            return True

        def returnNumberSI(text):
            text = "{:010.0f}".format(int(text))
            if len(text) > 10:
                raise ValueError
            return text 


        cleaned = text.remove_punctuation(self.clean()).replace(" ","")

        if isAllNumber(cleaned):
            cleaned = returnNumberSI(cleaned)
            return cleaned
        
        if re.match("0*[0-9]{7,10}[a-zA-Z]{0,3}$", cleaned):
            alpha_len = len([i for i in cleaned if i.isalpha()])
            number_comp = returnNumberSI(cleaned[:-alpha_len])
            cleaned = number_comp + cleaned[-alpha_len:]
            return cleaned
        else:
            raise

    def standardize(self,error = 'ignore', encrypt = False):
        try:
            res =  self.cleankey().upper()
        except:
            if error == 'ignore':
                res = self.value
            if error == 'nan':
                res =  np.nan

        if encrypt:
            res = text(res).encrypt()
        return res       



class PersonalTax(text):
    pass

class CorporateTax(text):
    pass
