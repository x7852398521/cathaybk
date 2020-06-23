# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

def col_eql(*c): #檢查表單欄位是否相等函式
    for i in range(len(c)):
        if i < len(c)-1:
            print('第{}與第{}個表單欄位是否完全相等：'.format(i+1,i+2),end='')
            print((c[i].columns == c[i+1].columns).all())
            
def trans(chn):  #中文轉阿拉伯數字
    digit = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
    def _trans(s):
        num = 0
        if s:
            idx_q, idx_b, idx_s = s.find('千'), s.find('百'), s.find('十')
            if idx_q != -1:
                num += digit[s[idx_q - 1:idx_q]] * 1000
            if idx_b != -1:
                num += digit[s[idx_b - 1:idx_b]] * 100
            if idx_s != -1:
                # 十前忽略一的處理
                num += digit.get(s[idx_s - 1:idx_s], 1) * 10
            if s[-1] in digit:
                num += digit[s[-1]]
        return num
    chn = chn.replace('零', '')
    idx_y, idx_w = chn.rfind('億'), chn.rfind('萬')
    if idx_w < idx_y:
        idx_w = -1
    num_y, num_w = 100000000, 10000
    if idx_y != -1 and idx_w != -1:
        return trans(chn[:idx_y]) * num_y + _trans(chn[idx_y + 1:idx_w]) * num_w + _trans(chn[idx_w + 1:])
    elif idx_y != -1:
        return trans(chn[:idx_y]) * num_y + _trans(chn[idx_y + 1:])
    elif idx_w != -1:
        return _trans(chn[:idx_w]) * num_w + _trans(chn[idx_w + 1:])
    return _trans(chn)

def cton(value): #去除文字"層"，並將中文轉阿拉伯數字
    if type(value)==str:
        c_num = value.replace('層','') #去除文字"層"
        c_num = trans(c_num)
        c_num = np.int(c_num)
        #np.set_printoptions(precision=0)
    elif type(value)==int or type(value)==float:
        c_num = value
    return c_num

df_a = pd.read_csv('a_lvr_land_a.csv',encoding='utf-8',header=0, skiprows=range(1,2), error_bad_lines=False)  #跳過英文標題、及報錯行(紀錄)
df_b = pd.read_csv('b_lvr_land_a.csv',encoding='utf-8',header=0, skiprows=range(1,2), error_bad_lines=False)
df_e = pd.read_csv('e_lvr_land_a.csv',encoding='utf-8',header=0, skiprows=range(1,2), error_bad_lines=False)
df_f = pd.read_csv('f_lvr_land_a.csv',encoding='utf-8',header=0, skiprows=range(1,2), error_bad_lines=False)
df_h = pd.read_csv('h_lvr_land_a.csv',encoding='utf-8',header=0, skiprows=range(1,2), error_bad_lines=False)

col_eql(df_a,df_b,df_e,df_f,df_h)  #檢查表單欄位是否相等

df_a['縣市']='臺北市'
df_b['縣市']='臺中市'
df_e['縣市']='高雄市'
df_f['縣市']='新北市'
df_h['縣市']='桃園市'

df_all = pd.concat([df_a,df_b,df_e,df_f,df_h], axis=0, ignore_index=True)  #合併資料為df_all
df_all.to_csv('df_all.csv', header=True, index=False, encoding='utf_8_sig')
print('合併前一共{}行資料'.format(len(df_a)+len(df_b)+len(df_e)+len(df_f)+len(df_h)))  #總資料行數
print('合併後一共{}行資料'.format(len(df_all))) #檢查合併前後數量是否一致

###列舉指定欄位內的資料種別
print(set(df_all['主要用途']))
print(set(df_all['建物型態']))
print(set(df_all['總樓層數']))

###資料篩選，輸出filter_a.csv
#df_all['總樓層數'].fillna(value='None', inplace=True) #將Nan變成''
filter1 = df_all['主要用途'] == '住家用'
filter2 = df_all['建物型態'].str.contains('住宅大樓')
filter3 = df_all['總樓層數'].apply(cton)>=13
#filter3 = df_all['總樓層數'].str[:-1]  #去除最後一個字"層"
filter_a = df_all[(filter1 & filter2 & filter3)] 
filter_a.to_csv('filter_a.csv', header=True, index=False, encoding='utf_8_sig')

###資料計算，輸出filter_b.csv
all_count = filter_a.count().max()
car_count = filter_a['交易筆棟數'].str.extract(pat='(車位)(\d+)', flags=0, expand=True)[1].astype('int').sum()
avg_price = round(filter_a[filter_a['總價元']>0]['總價元'].sum() / filter_a[filter_a['總價元']>0]['總價元'].count(),2)
avg_carprice = round(filter_a[filter_a['車位總價元']>0]['車位總價元'].sum() / filter_a[filter_a['車位總價元']>0]
                     ['交易筆棟數'].str.extract(pat='(車位)(\d+)', flags=0, expand=True)[1].astype('int').sum(),2)
b_data = [{'總件數':all_count,'總車位數':car_count,'平均總價元':avg_price,'平均車位總價元':avg_carprice}]
filter_b = pd.DataFrame(b_data)
filter_b.to_csv('filter_b.csv', header=True, index=False, encoding='utf_8_sig')













