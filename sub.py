# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 10:52:50 2021

@author: JawadShaik
"""

#from org.apache.nifi.processors.script import ExecuteScript
#from org.apache.nifi.processor.io import InputStreamCallback
#from java.io import BufferedReader, InputStreamReader
import paho.mqtt.client as mqttClient
#import time
from datetime import datetime
import psycopg2
#import smtplib
#from email.message import EmailMessage
import win32com.client


Connected = False

def main():
    client = mqttClient.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("127.0.0.1", 1883, 60)
    client.loop_forever()

# The callback function of connection
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("compressor/profilecheck")

# The callback function for received message
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload.decode()))
    insert_into_postgresql(str(msg.payload.decode()))
    
def insert_into_postgresql(msg):
    try:
        x = msg.split(",")
        print(x)
        dt = datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S.%f')
        connection = psycopg2.connect(user="postgres",
                                      password="Juaims@786",
                                      host="127.0.0.1",
                                      port=5432,
                                      database="postgres")
        cursor = connection.cursor()
         
        print(x)
        #if int(x[1]) < 40:
        #    send_alert_out()
            #server='https://outlook.office365.com/owa/aigcom.com/'
            #from_email='jubail_alerts@nomac.com'
            #from_email='j.shaik@aigcom.com'
            #print('before sending alert')
            #send_alert('j.shaik@aigcom.com','Nomac Jubail Plant Alerts', 'compressor pressure above threshold',server,from_email)
            #print('after sending alert')
        #else:
        #    print('no alert')       
        postgres_insert_query = """ INSERT INTO compressor_profile_check (timestep, discharge_flow,power_consumption,stage1_bearing_temperature,stage1_suction_temperature,
        stage2_bearing_temperature,stage3_bearing_temperature,stage4_bearing_temperature,discharge_temperature,oil_temperature_after_cooler,
        motor_bearing_temperature_drive_end,motor_bearing_temperature_n1_drive_end,main_shaft_axial_movement,stage1_bearing_vibration,stage2_bearing_vibration,
        stage3_bearing_vibration,stage4_bearing_vibration,inlet_guide_vane_position,isflds,comp_class) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"""
        record_to_insert = (dt, x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], x[2], x[1])
        
        print(record_to_insert)
        cursor.execute(postgres_insert_query, record_to_insert)    
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into compressor profile check table table")
    
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into heat exchanger table", error)
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    return

def send_alert_out():
    outlook = win32com.client.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = 'j.shaik@aigcom.com'
    mail.Subject = 'Nomac Jubail Plant Alerts'
    mail.HTMLBody = '<h3>compressor pressure above threshold</h3>'
    mail.Body = "compressor pressure above threshold"
    #mail.Attachments.Add('c:\\sample.xlsx')
    #mail.Attachments.Add('c:\\sample2.xlsx')
    #mail.CC = 'somebody@company.com'
    mail.Send()
    return

def send_alert(to_email, subject, message, server,from_email):
    # import smtplib
    print('1')
    msg = EmailMessage()
    print(msg)
    print('2')
    msg['Subject'] = subject
    print(msg)
    print('3')
    msg['From'] = from_email
    print(msg)
    print('4')
    #msg['To'] = ', '.join(to_email)
    msg['To'] = to_email
    print(msg)
    print('5')
    msg.set_content(message)
    print('6')
    print(msg)
    server = smtplib.SMTP(server)
    print('7')
    server.set_debuglevel(1)
    print('8')
    server.login(from_email, 'password')  # user & password
    print('9')
    server.send_message(msg)
    print('10')
    server.quit()
    print('11')
    print('successfully sent the mail.')
    return
    
if __name__ == "__main__":
    main()

