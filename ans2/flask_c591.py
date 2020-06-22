# -*- coding: utf-8 -*-
from flask import Flask, render_template   
import pymongo
import json
import c591
import re

app = Flask(__name__)

@app.route('/')
def show_all():
    a=[]
    myclient = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = myclient['house']
    collection = mydb['data']
    myquery = {}
    results = collection.find(myquery,{'_id':0})
    for result in results:
        a.append(result)
    return json.dumps(a, ensure_ascii=False, indent=4)
  
@app.route('/search/<string:filt>') #filt = '出租者性別=女&縣市=新北'
def show_search(filt):
    h = c591.housep()
    filt_list = re.split('=|&',filt) 
    filt_dict = {}
    for i in range(len(filt_list)):
        if i < len(filt_list)-1:
            if i%2 == 0:
                filt_dict[filt_list[i]]=filt_list[i+1]
    a = h.srh(filt_dict)  #{'出租者性別':'女', '縣市':'新北'}
    return json.dumps(a, ensure_ascii=False, indent=4)
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)

