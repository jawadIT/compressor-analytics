# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 10:50:02 2021

@author: JawadShaik
"""

import paho.mqtt.client as mqttClient
import time
from datetime import datetime
import pandas as pd
import joblib
import numpy as np
from pathlib import Path

global Connected
Connected = False

def main():
    client = mqttClient.Client()
    client.on_connect = on_connect
    client.connect("127.0.0.1", 1883, 60)
    client.loop_start() 
    
    df = pd.read_csv('Compressor_TrainData.csv')
    old_names = df.columns
    new_names = ['ts','df','dfa','pc','bt1','st','bt2','bt3','bt4','dt','otac','mbtde','mbtnde','msam','bv1','bv2','bv3','bv4','igvp']
    df.rename(columns=dict(zip(old_names, new_names)), inplace=True)     
    
    dfr = pd.read_csv('cluster_ranges.csv')
    dfr.reset_index(drop=True, inplace=True)
    dfd = pd.read_csv('long_names.csv')
    
    #filename = r'C:\Use cases - realtime data\Heat Exchangers\Real-Time Data._tmp.xls'
    #filename = Path(filename)
    
    #set features and load models
    comp_features = ['df','pc','bt1','st','bt2','bt3','bt4','dt','otac','mbtde','mbtnde','msam','bv1','bv2','bv3','bv4','igvp']
    
    
    comp_scaler = joblib.load('comp_scaler.save')
    comp_classifier = joblib.load('comp_dt_cl.save')
        
   
    while Connected != True:                         
        time.sleep(2)
        
    try:
        while True:     
            #print('i am here')
            ##try:                
                ##with open(filename, 'rb') as handle: 
                    #print('i am here in file read')
                    ##dfraw = pd.read_excel(handle)
                    #print('after file read')                            
                    ##df = rename(dfraw)
            profile_check(df,dfr,dfd,comp_features,comp_scaler,comp_classifier,client)
                #print('prediction successfull')
            ##except:
                ##print('error in file access or prediction')
            #now = datetime.now()
            ##timestamp = datetime.timestamp(now)
            #strng = str(now) + ',' + str(clust_pred)
            #client.publish("compressor/pressure", payload=strng, qos=0, retain=False)   
    except KeyboardInterrupt:     
        client.disconnect()
        client.loop_stop()
        
    #value = input('Enter the message:')
    #client.publish("khobar/adeertower", payload=value, qos=0, retain=False)
    time.sleep(2)
    
    #for i in range(3):
    #    client.publish("khobar/adeertower", payload=i, qos=0, retain=False)
    #    print(f"send {i} to a/b")
    #    time.sleep(1)
    
    client.loop_forever()
    
#get features out of range
def check_ranges(dfpred,dfr,dfd,comp_features):
    
    isflds = ''
    for col in comp_features:
        if ((list(dfpred[col])[0] < list(dfr[dfr['colmn']==col]['sv'])[0]) or 
            (list(dfpred[col])[0] > list(dfr[dfr['colmn']==col]['ev'])[0])):
            isflds = isflds + '\n' + str(list(dfd[dfd['shrtname']==col]['long_name'])[0])
    
    return isflds

#real time predictions
def profile_check(df,dfr,dfd,comp_features,comp_scaler,comp_classifier,client):
    dfpred = df.sample(n=1)
   
    #print('0')    
    comp_class = comp_classifier.predict(comp_scaler.transform(dfpred[comp_features]))[0]    
    dfpred['comp_class_pred'] = comp_class
   
    
    #print('3')
    if comp_class == 0:      
        dft = dfr[dfr['clust']==0]
        isflds = check_ranges(dfpred,dft,dfd,comp_features)
        dfpred['isflds'] = isflds
    else: 
        if comp_class == 1:
            dft = dfr[dfr['clust']==1]
            isflds = check_ranges(dfpred,dft,dfd,comp_features)
            dfpred['isflds'] = isflds
        else:
            if comp_class == 2:
                dft = dfr[dfr['clust']==2]
                isflds = check_ranges(dfpred,dft,dfd,comp_features)
                dfpred['isflds'] = isflds
            else:
                if comp_class == 3:
                    dft = dfr[dfr['clust']==3]
                    isflds = check_ranges(dfpred,dft,dfd,comp_features)
                    dfpred['isflds'] = isflds
                else:
                    dft = dfr[dfr['clust']==4]
                    isflds = check_ranges(dfpred,dft,dfd,comp_features)
                    dfpred['isflds'] = isflds
    
    now = datetime.now()
    
    strng = str(now) + ',' + str(comp_class) + ',' + str(isflds)  
    
    #print('7')
    for f in comp_features:
        strng = strng + ',' + str(np.array(dfpred[f])[0])
    print(strng)
        
   # print('8')
    client.publish("compressor/profilecheck", payload=strng, qos=0, retain=False)  
    time.sleep(20)

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    if rc == 0:
        #print("Connected to broker")
        global Connected                                
        Connected = True                                
    else:
        print("Connection failed")

if __name__ == "__main__":
    main()

