# -*- coding: utf-8 -*-
import requests #pip install requests
import bs4 #pip install beautifulsoup4  #pip install lxml
import math
import re
import pymongo #pip install pymongo

class housep():
    def __init__(self):         
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Host': 'rent.591.com.tw',
            'Referer': 'https://rent.591.com.tw/?kind=0&region=3',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36',
            #'X-CSRF-TOKEN': X_token,
            'X-Requested-With': 'XMLHttpRequest'
            }        
    def inputdb(self,jsond): #匯入資料庫
        myclient = pymongo.MongoClient('mongodb://localhost:27017/')
        mydb = myclient['house']
        collection = mydb['data']
        for j in jsond:
            try:
                collection.insert_one(j)
            except:
                pass
    def dele_repeat(self):
        del_num = 0
        myclient = pymongo.MongoClient('mongodb://localhost:27017/')
        mydb = myclient['house']
        collection = mydb['data']
        myquery = {}
        results = collection.find(myquery,{ "_id": 0 })
        for result in results:
            del_num += 1
            print('刪除進度：',del_num,'/',collection.count_documents({}))
            if collection.count_documents({'網址':result['網址']})>1:
                self.del_rep = collection.delete_one({'網址':result['網址']})
    def srh(self,dic={}):
        myclient = pymongo.MongoClient('mongodb://localhost:27017/')
        mydb = myclient['house']
        collection = mydb['data']
        dic1 = {}
        a=[]
        column = {'縣市','出租者','出租者身份','聯絡電話','型態','現況','性別要求'}
        renter = {'出租者姓氏','出租者性別'}
        word = ln = fn =''
        for i in dic.keys():
            if i in renter:
                if str(dic[i])[0] == 'n':
                    if i == '出租者性別':
                        if str(dic[i])[1] == '男':
                            fn = '[^先生]$'
                        elif str(dic[i])[1] == '女':
                            fn = '[^小姐|媽媽|太太]$'
                    elif i == '出租者姓氏':
                        ln = '^[^' + str(dic[i])[1:] + ']'
                else:
                    if i == '出租者性別':
                        if dic[i] == '男':
                            fn = '[先生]$'
                        elif dic[i] == '女':
                            fn = '[小姐|媽媽|太太]$'
                    elif i == '出租者姓氏':
                        ln = '^[' + dic[i] + ']'
                        
            elif i in column:
                if str(dic[i])[0] == 'n':
                    word = '^((?!' + str(dic[i])[1:] + ').)*$'
                else:
                    word = '.*'+dic[i]+'.*' 
                dic1.update({i: {'$regex': word}})
    
            if renter.issubset(dic.keys()) and len(ln)>0 and len(fn)>0:
                word = ln + '.' + fn
                dic1.update({'出租者': {'$regex': word}})
            elif not renter.issubset(dic.keys()) and len(ln)>0:
                word = ln + '.*$'
                dic1.update({'出租者': {'$regex': word}})
            elif not renter.issubset(dic.keys()) and len(fn)>0:
                word = '.*' + fn
                dic1.update({'出租者': {'$regex': word}})
        results = collection.find(dic1,{ "_id": 0 })
        for result in results:
            a.append(result)
        return a            
    def urlcheck(self,data_url):
        myclient = pymongo.MongoClient('mongodb://localhost:27017/')
        mydb = myclient['house']
        collection = mydb['data']
        myquery = { "網址":  data_url } #查詢網址欄位
        if collection.count_documents(myquery): #假如搜尋到至少一筆資料
            return 1
        else:
            return 0
    def dele(self,data_url_list):
        del_num = 0
        num = 0
        myclient = pymongo.MongoClient('mongodb://localhost:27017/')
        mydb = myclient['house']
        collection = mydb['data']
        filter_text = '^[' + self.region_text + '].*$'
        myquery = {'縣市': {'$regex': filter_text}}
        results = collection.find(myquery)
        for result in results:
            if result['網址'] in data_url_list:
                continue
            elif result['網址'] not in data_url_list:
                myquery = {'網址': result['網址']}
                collection.delete_many(myquery)
                del_num += 1
            num += 1
            print('刪除進度：',num)
        print(del_num, "筆資料已刪除")   
    def inall(self): #爬取與更新租屋物件
        self.update_num = 0
        self.urls_list = []
        self.total = []
        def dnd(page,region):
            all_data = []
            #台北region=1，新北region=3
            #每30筆1頁，第一頁page='0'，第二頁page='30'
            url = 'https://rent.591.com.tw/?kind=0&region={}&order=posttime&orderType=desc/search/rsList?is_new_list=1&type=1&kind=0&searchtype=1&region={}&firstRow={}'.format(self.region,self.region,page)            
            html_1 = requests.get(url, headers=self.headers)
            Soup_1 = bs4.BeautifulSoup(html_1.text, 'lxml')
            #url_1 = Soup_1.find('section',{'class':'listBox'}).find('div',{'id':'content'}).find_all('a',target="_blank")
            #此時發現多搜尋5筆資料，利用Xpath輸入//section[@class='listBox']//div[@id='content']//a[@target='_blank']搜尋其餘5筆資料，故修正為以下搜尋方式
            url_1 = Soup_1.find('section',{'class':'listBox'}).find('div',{'id':'content'}).find_all('li',{'class':'infoContent'}) #擷取租房清單'url'
            self.region_text = Soup_1.find('span',{'class':'search-location-span','data-index':'1'}).text.split()[0].strip() #擷取'縣市'名稱
            url_list=[]
            ainfo_list=[]
            info_list=[]
            #擷取出租者與出租者身分
            for i in url_1:  
                url_list.append(i.a['href'])
                ainfo_list.append(i.text.split())
                info_list.append(i.find_all('em')[1].text.strip().split())
            self.urls_list += url_list
            for j in range(len(info_list)):
                if self.urlcheck(url_list[j]):  #url_list
                    print('執行進度：',int(page)+j+1,'/',self.page*30) 
                    continue
                else:
                    self.update_num += 1
                info2_list={}
                info3_list={}
                #擷取聯絡電話、型態、現況Soup
                headers2 =  { 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
                   AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'} 
                try:
                    html_2 = requests.get('http:'+url_list[j], headers=headers2)
                except:
                    print('執行進度：',int(page)+j+1,'/',self.page*30) 
                    continue
                Soup_2 = bs4.BeautifulSoup(html_2.text, 'lxml')
                Soup_2.encoding = 'utf-8'
                #縣市
                try:
                    info2_list['縣市'] = self.region_text
                except:
                    info2_list['縣市'] = '-'
                #出租者
                info2_list['出租者'] = info_list[j][1]
                #出租者身份
                info2_list['出租者身份'] = info_list[j][0]
                #擷取電話 利用chromepath查xpath很方便
                try:
                    phone = Soup_2.find('span',{'class':'dialPhoneNum'})['data-value'].strip()
                    info2_list['聯絡電話'] = phone
                except:
                    info2_list['聯絡電話'] = '-'
                #擷取型態與現況
                g = Soup_2.find('ul',{'class':'attr'}).find_all('li')
                for k in g:
                    info = k.text.split()
                    info3_list[info[0]] = info[2]
                try :
                    info2_list['型態'] = info3_list['型態']
                except:
                    info2_list['型態'] = '-'
                try :
                    info2_list['現況'] = info3_list['現況']
                except:
                    info2_list['現況'] = '-'
                #性別要求
                if re.search('性別要求：(男生|女生|男女生皆可)',Soup_2.text,re.S):  #查找內文是否含"性別要求"，要找所有可以修改為re.findall
                    gender = re.search('性別要求：(男生|女生|男女生皆可)',Soup_2.text,re.S).group(1)
                    info2_list['性別要求'] = gender
                else:
                    info2_list['性別要求'] = '男女生皆可'
                #網址
                info2_list['網址'] = url_list[j]
                
                print('執行進度：',int(page)+j+1,'/',self.page*30)
                all_data.append(info2_list) #所有屬性資料存入all_data
            return all_data

        self.region = input('輸入欲搜尋的城市(1.台北 2.新北)：')
        if not self.region.isdigit():
            raise Exception('請輸入整數')
        elif int(self.region)<1:
            raise Exception('數值應介於1~2')
        elif int(self.region)>2:
            raise Exception('數值應介於1~2')
        elif int(self.region)==1:
            self.headers.update({'Cookie':'webp=1; PHPSESSID=jl8gdf3pkqi146krgj9lva8i71; T591_TOKEN=jl8gdf3pkqi146krgj9lva8i71; _ga=GA1.3.1886459399.1592324889; _gid=GA1.3.271853252.1592324889; _gid=GA1.4.271853252.1592324889; _ga=GA1.4.1886459399.1592324889; tw591__privacy_agree=0; _fbp=fb.2.1592324890666.688340931; user_index_role=1; __auc=95df8efb172be150f030765be80; localTime=2; new_rent_list_kind_test=0; __utmz=82835026.1592327171.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmc=82835026; is_new_index=1; is_new_index_redirect=1; __utma=82835026.1886459399.1592324889.1592368685.1592384998.3; pt_28697119=uid=W152iQxl7b0datp9-HhETQ&nid=1&vid=JzeIpMEz5VaZXltPkNnitA&vn=1&pvn=1&sact=1592399938763&to_flag=0&pl=Wu1VaI9MZHUWMkxuR2Du/A*pt*1592399938763; pt_s_28697119=vt=1592399938763&cad=; index_keyword_search_analysis=%7B%22role%22%3A%221%22%2C%22type%22%3A%222%22%2C%22keyword%22%3A%22%E5%AE%89%E6%9D%B1%E8%A1%9745%E8%99%9F9%E6%A8%93%E4%B9%8B9%22%2C%22selectKeyword%22%3A%22%22%2C%22menu%22%3A%22%22%2C%22hasHistory%22%3A1%2C%22hasPrompt%22%3A0%2C%22history%22%3A2%7D; c10f3143a018a0513ebe1e8d27b5391c=1; last_search_type=1; DETAIL[1][9423871]=1; DETAIL[1][9423850]=1; user_browse_recent=a%3A5%3A%7Bi%3A0%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229423850%22%3B%7Di%3A1%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229423871%22%3B%7Di%3A2%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229423907%22%3B%7Di%3A3%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229422140%22%3B%7Di%3A4%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229422136%22%3B%7D%7D; ba_cid=a%3A5%3A%7Bs%3A6%3A%22ba_cid%22%3Bs%3A32%3A%22c17a4ba5a94f1f026a009772ab750907%22%3Bs%3A7%3A%22page_ex%22%3Bs%3A48%3A%22https%3A%2F%2Frent.591.com.tw%2Frent-detail-9423871.html%22%3Bs%3A4%3A%22page%22%3Bs%3A48%3A%22https%3A%2F%2Frent.591.com.tw%2Frent-detail-9423850.html%22%3Bs%3A7%3A%22time_ex%22%3Bi%3A1592578028%3Bs%3A4%3A%22time%22%3Bi%3A1592579518%3B%7D; _gat=1; _gat_UA-97423186-1=1; XSRF-TOKEN=eyJpdiI6ImR0TE9BUVdPZk9DcFVtRURLWEUxWkE9PSIsInZhbHVlIjoiU3ZrbFk2QXp2Umh3QUo0Mm45Vm5aVTlGdVkyc1wvbnRuSkg2Q1o0WDlKOUs4Wm1WdU1xd2JMS2ZyS2h6UkhiUllud3pNRlBLSytXNnlPR0J2VGp1dHZ3PT0iLCJtYWMiOiIzNjNjNWU3ZGY2ZTIwYzlmMTRhNDA4NmYyYjhmOGE2NDYwZDc0OTk1MmVmOTIyYzQ2YTUzYmVjYmUyNDMzN2ZkIn0%3D; 591_new_session=eyJpdiI6Ijc1WnVlQUs1KzVRS3ZZV1JTNFNzTEE9PSIsInZhbHVlIjoiazg0QUtrejVjTGNIZU5rbXUrVlhtOU5EWFczWDhaUExpK0FRYmJqVFhZSnFLN3pFMUNVNkVXRGhUeDlwZWp1TDVoUFlDRGZ4QVp1QzUxS2tPYXJUbUE9PSIsIm1hYyI6ImVhYzE5NDAwNTUxMTAxN2FiODQ3YmEyOThlYTYyMTgwZTBlOWUzODZiZTYwODdmNzUxZDYzNTg1OTIwYTI3MjAifQ%3D%3D; urlJumpIp=1; urlJumpIpByTxt=%E5%8F%B0%E5%8C%97%E5%B8%82'})
        elif int(self.region)==2:
            self.region = '3'
            self.headers.update({'Cookie':'webp=1; PHPSESSID=jl8gdf3pkqi146krgj9lva8i71; T591_TOKEN=jl8gdf3pkqi146krgj9lva8i71; _ga=GA1.3.1886459399.1592324889; _gid=GA1.3.271853252.1592324889; _gid=GA1.4.271853252.1592324889; _ga=GA1.4.1886459399.1592324889; tw591__privacy_agree=0; _fbp=fb.2.1592324890666.688340931; user_index_role=1; __auc=95df8efb172be150f030765be80; localTime=2; new_rent_list_kind_test=0; __utmz=82835026.1592327171.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmc=82835026; is_new_index=1; is_new_index_redirect=1; __utma=82835026.1886459399.1592324889.1592368685.1592384998.3; pt_28697119=uid=W152iQxl7b0datp9-HhETQ&nid=1&vid=JzeIpMEz5VaZXltPkNnitA&vn=1&pvn=1&sact=1592399938763&to_flag=0&pl=Wu1VaI9MZHUWMkxuR2Du/A*pt*1592399938763; pt_s_28697119=vt=1592399938763&cad=; index_keyword_search_analysis=%7B%22role%22%3A%221%22%2C%22type%22%3A%222%22%2C%22keyword%22%3A%22%E5%AE%89%E6%9D%B1%E8%A1%9745%E8%99%9F9%E6%A8%93%E4%B9%8B9%22%2C%22selectKeyword%22%3A%22%22%2C%22menu%22%3A%22%22%2C%22hasHistory%22%3A1%2C%22hasPrompt%22%3A0%2C%22history%22%3A2%7D; c10f3143a018a0513ebe1e8d27b5391c=1; last_search_type=1; DETAIL[1][9423871]=1; DETAIL[1][9423850]=1; user_browse_recent=a%3A5%3A%7Bi%3A0%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229423850%22%3B%7Di%3A1%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229423871%22%3B%7Di%3A2%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229423907%22%3B%7Di%3A3%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229422140%22%3B%7Di%3A4%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A1%3Bs%3A7%3A%22post_id%22%3Bs%3A7%3A%229422136%22%3B%7D%7D; ba_cid=a%3A5%3A%7Bs%3A6%3A%22ba_cid%22%3Bs%3A32%3A%22c17a4ba5a94f1f026a009772ab750907%22%3Bs%3A7%3A%22page_ex%22%3Bs%3A48%3A%22https%3A%2F%2Frent.591.com.tw%2Frent-detail-9423871.html%22%3Bs%3A4%3A%22page%22%3Bs%3A48%3A%22https%3A%2F%2Frent.591.com.tw%2Frent-detail-9423850.html%22%3Bs%3A7%3A%22time_ex%22%3Bi%3A1592578028%3Bs%3A4%3A%22time%22%3Bi%3A1592579518%3B%7D; _gat=1; urlJumpIp=3; urlJumpIpByTxt=%E6%96%B0%E5%8C%97%E5%B8%82; _gat_UA-97423186-1=1; XSRF-TOKEN=eyJpdiI6IlJXTEVPS3JaYk9WZENnSDFFczZpNGc9PSIsInZhbHVlIjoiWUlaODBpSHFIOWlyMExIbEpLQWJCb0h0YXpmQUljNE9zSktqN2lMckNQOTJyZ0I0c0crXC85THBidkxubVQweVRUOENZempWSVlTaU5oZDNCQ05YRkhnPT0iLCJtYWMiOiIxMDU2NDk1MTY4YjVjMjU1MTE2NWNkMzZhYzcxZGU0YWVmZjk3ODZmZDZlN2EzY2I3YThiZDUyOTFkNWU4NTc3In0%3D; 591_new_session=eyJpdiI6IlN1TDI3UTEwdU1vbWRQbEJZeE9ISnc9PSIsInZhbHVlIjoidDI1Zm44TDlpU1diY0trRTVcL1wvek5HTGdrcDJ0WUNwa3hkXC9pTENQMHU0NjlvTm1PMjkzcTRMdlRlNWhRS3B3dGc0Q3NzWElsb2JVc2gya0QzSllPWmc9PSIsIm1hYyI6ImI0M2UyODFmOTQ4NGE1MzM3OTNhZGNlZGJiNjY3ZTNiNTNiMmIxYmYwZWNhZWRkZTY0MDgwOWY4MjI5YTQyZjYifQ%3D%3D'})
        
        firstRow = '0' #第一頁
        url = 'https://rent.591.com.tw/?kind=0&region={}&order=posttime&orderType=desc/search/rsList?is_new_list=1&type=1&kind=0&searchtype=1&region={}&firstRow={}'.format(self.region,self.region,firstRow)
        html = requests.get(url, headers=self.headers)
        Soup = bs4.BeautifulSoup(html.text, 'lxml')
        self.t_num = int(Soup.find('div','pageBar').find('span',{'class':'R'}).text.strip()) #資料總筆數
        p_num = math.ceil(self.t_num/30) #資料總頁數
        print('總頁數：',p_num)
        
        self.page = input('輸入抓取總頁數(請輸入數值介於{}~{}之間)：'.format('1',p_num))
        if not self.page.isdigit():
            raise Exception('請輸入整數')
        elif int(self.page)<1:
            raise Exception('數值應介於1~{}'.format(p_num))
        elif int(self.page)>p_num:
            raise Exception('數值應介於1~{}'.format(p_num))
        else:
            self.page = int(self.page)

        for k in range(1,self.page+1):
            try:
                self.total = []  ###new新增
                data=[] #new新增
                data = dnd(str((k-1)*30),self.region)
                self.total += data #self.total為所有擷取的資料
            except:
                continue
            if len(self.total) == 0:###new縮排
                print('共新增0筆資料')###new縮排
            else:###new縮排
                print('共新增{}筆資料'.format(self.update_num))###new縮排
                self.inputdb(self.total)###new縮排
        del_yn = input('是否刪除載入以外的資料(y/n)：')###new未縮排
        if del_yn == 'y':###new未縮排
            self.dele(self.urls_list)###new未縮排
        else:###new未縮排
            pass###new未縮排

if __name__ == '__main__':
    h = housep()
    h.inall() #爬取與更新租屋物件
    h.srh() #查詢所有物件
    h.srh({'出租者性別':'男', '縣市':'新北'}) #【男生可承租】且【位於新北】的租屋物件
    h.srh({'聯絡電話':'0933-746-608'}) #以【聯絡電話】查詢租屋物件
    h.srh({'出租者身份':'n屋主'}) #所有【非屋主自行刊登】的租屋物件
    h.srh({'縣市':'台北','出租者性別':'女','出租者姓氏':'吳'}) #【臺北】【屋主為女性】【姓氏為吳】所刊登的有租屋物件
    
    
    
    
    
    
    