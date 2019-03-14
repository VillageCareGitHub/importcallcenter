# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 14:24:01 2018

@author: Eric Hall
Description: Created Mitel List for Call Center
Querying the REDSHIFT database. Created a view to capture
all members within the Mitel File Specifications
"""

import boto3
from botocore.exceptions import ClientError
import psycopg2
import csv
#Code for opening up file from s3 bucket
#s3=boto3.resources('s3')
#with open('filename','wb') as data:
 #   s3.download_fileobj('','',data)
# Initialize configuration settings
import configparser
initConfig = configparser.ConfigParser()
initConfig.read("Mitel_List.config")
 
#Connect to Redshift and gather data
conn=psycopg2.connect(dbname= 'vcdwh', host=initConfig.get('profile prod', 'host'), 
port= initConfig.get('profile prod', 'port'), user= initConfig.get('profile prod', 'dbuser'), password= initConfig.get('profile prod', 'dbpwd'))
   
cur = conn.cursor()
#Creating CSV file with Mitel data from Redshift
cur.execute('select * from reporting.vw_CallCenter_Members_nodup')

all_cases=cur.fetchall()
rownumcount=0 
with open('Mitel_List.csv', 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            #Write Header for CSV
            header=('uniqueid','name','email','phone','mobile')
            wr.writerow(header)
            for row in all_cases:
    
                rownumcount=rownumcount+1
        
                wr.writerow(row)
        




    
#Uploading File to S3 bucket

                
                

s3 = boto3.client(
    's3',
    aws_access_key_id=initConfig.get('profile prod', 'aws_access_key_id') ,
    aws_secret_access_key=initConfig.get('profile prod', 'aws_secret_access_key'))

s3.upload_file("Mitel_List.csv", "vc-extn-mitel", "Mitel_List.csv")

#Closing RedShift Connections
cur.close()
conn.close()

#Email Information
# Get current date
import datetime
currentDay = datetime.datetime.today().strftime('%Y%m%d')

# Initialize SES email client
client = boto3.client('ses',region_name="us-east-1",aws_access_key_id=initConfig.get('profile prod', 'aws_access_key_id') ,
    aws_secret_access_key=initConfig.get('profile prod', 'aws_secret_access_key'))

def sendEmail(SUBJECT, BODY_TEXT, BODY_HTML):
    try:
    	CHARSET = "UTF-8"
    	response = client.send_email(
    		Destination={'ToAddresses': [
    			initConfig.get('messaging', 'emailto'),
    		],},
    		Message={
    			'Body': {
    				'Html': {'Charset': CHARSET,'Data': BODY_HTML,},
    				'Text': {'Charset': CHARSET,'Data': BODY_TEXT,},
    			},
    			'Subject': {'Charset': CHARSET,'Data': SUBJECT,},
    		},
    		Source=initConfig.get('messaging', 'emailfrom'),
    	)
        
    except ClientError as e:
      print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])  

def sendSuccessNotification(rowCount):
    
	SUBJECT = currentDay + " Daily Mitel Export finished successfully"
	BODY_TEXT = ("Daily Mitel Export finished successfully\r\n"
		"Daily Mitel Export for " + currentDay) + ". " + str(rowCount) + " rows exported"
	BODY_HTML = "<html><head></head><body><h1>" \
		+ currentDay + " Daily Mitel Export finished successfully</h1><p>Files processed:<p>"
	
	BODY_HTML = BODY_HTML + "Mitel_List.csv on the AWS S3 Bucket vc-extn-mitel "+ "<p>"
	BODY_HTML = BODY_HTML + str(rowCount) + " rows exported<p>"
	BODY_HTML = BODY_HTML + "</body></html>"
	sendEmail(SUBJECT, BODY_TEXT, BODY_HTML)  
    
    
# Send notification email

sendSuccessNotification(rownumcount)

