#Importamos librerias
from datetime import datetime, timedelta, date
from pymongo.errors import BulkWriteError
from configparser import ConfigParser
import BluekaiAudienceConnect as bk #Traducción by Hexagon
from urllib.request import urlopen
from urllib.parse import urlparse
from time import process_time
import pandas as pd
import pymongo
import urllib
import json
import sys
import time
parser = ConfigParser()
parser.read('config.cfg')
DB_USER = parser.get('DB', 'DB_USER')
DB_PASSWORD = parser.get('DB', 'DB_PASSWORD')
NODE_ENV = parser.get('ENVIRONMENT', 'NODE_ENV')
bkuid = parser.get('api_samples_bluekai', 'bkuid')
bksecretkey = parser.get('api_samples_bluekai', 'bksecretkey')
partner_id = parser.get('api_samples_bluekai', 'pid')
aud_id = parser.get('api_samples_bluekai', 'aud_id')
# bkuid = '72c487d4f0318706ce96a0ebe47b906342cb0a2e3610fe182746602c3be6d016' #Web Service User Key
# bksecretkey = '90388366ed94cfaea8ee163e0c64200a587974ce7492ec827b33e84ba644bad0' #Web Service Private Key
# partner_id = "3740"

Url = parser.get('api_samples_bluekai', 'Url')
# Partner ID: 6132 // Audience ID: 466546
# Web Service User Key:	72c487d4f0318706ce96a0ebe47b906342cb0a2e3610fe182746602c3be6d016	 
# Web Service Private Key:	90388366ed94cfaea8ee163e0c64200a587974ce7492ec827b33e84ba644bad0

AUD = bk.BluekaiAudienceCall(Url, "GET")
JsonList = AUD.ListAudience(partner_id, bkuid, bksecretkey)

print ("\nResponse Code: 200")

audiences = JsonList["audiences"]
# print(audiences)
audience_names = []
audience_id = []
category = []
categorieDiscmongo = []
categorieInsert = []
# if NODE_ENV == 'production' :
#         myclient = pymongo.MongoClient('mongodb+srv://'+ DB_USER +':'+ DB_PASSWORD +'@audiencekit1-76o4f.mongodb.net/audienceKit?retryWrites=true&w=majority')
#         mydb = myclient["bluekaiconnector"]
#         audienceColl = mydb["audieances"]
# elif NODE_ENV == 'development' :
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["bluekaiconnector"]
audienceColl = mydb["audiences"]
reportsColl = mydb["reports"]
categoriesColl = mydb["categories"]

'''recorre audiencias para sacar audience ids'''
for a in audiences:
    # audience_names.append(a["name"])
    audience_id.append(a["id"])
    date_audience = datetime.strptime(a['updated_at'],'%Y-%m-%dT%H:%M:%S+%f')
    dateAudienceCreate = datetime.strptime(a['created_at'],'%Y-%m-%dT%H:%M:%S+%f')
    now = datetime.now()
    now2 = datetime.today()
    # now3 = now2 - timedelta(days=6000)
    now3 = now2 - timedelta()
    print("id ------->",a['id'])


    if mydb.audiences.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience < now3 : 
                print("ya existe el id y la fecha " + date_audience.strftime('%Y-%m-%dT%H:%M:%S+%f')+ " es menor a " + now2.strftime('%Y-%m-%dT%H:%M:%S+%f')) 
    elif mydb.audiences.count_documents({ 'audienceId': a['id'] }, limit = 1) == 0 :
                    audienceColl.insert_one({
                            'audienceId': a['id'],
                            'audienceName': a['name'],
                            'audienceCreationDate': dateAudienceCreate,
                            'audienceModifiedDate': date_audience,
                            'audienceCountryCodes': a['countryCodes'],
                            'audienceDeviceType': a['device_type'],
                            'clientId': partner_id,
                            "recency" : a['recency'],
                            'typePlatform': 3,
                            'namePlatform': 'bluekai',
                            'segment':'',
                            'reach':''
                    })
                    print("inserta audiencias de", a['id'])
    elif mydb.audiences.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience > now3 :
    
                    audienceColl.insert_one({
                        # print(a['id']) 
                        # print(a['name']) 
                        # print(a['updated_at'])
                        # print(a['countryCodes'])
                        # print(a['idTypes'])
                        # print(a['recency'])
                        # print("segments", a['segments'])
                        # print("status", a['status'])
    
                            'audienceId': a['id'],
                            'audienceName': a['name'],
                            'audienceCreationDate': dateAudienceCreate,
                            'audienceModifiedDate': date_audience,
                            'audienceCountryCodes': a['countryCodes'],
                            'audienceDeviceType': a['device_type'],
                            'clientId': partner_id,
                            "recency" : a['recency'],
                            'typePlatform': 3,
                            'namePlatform': 'bluekai',
                            'segment':'', 
                            'reach':''                        
                    })
                    print("actualiza audiencias", a['id']) 

    #si la fecha de modificacion de la audiencia es reciente y ya existe el id 
    if mydb.reports.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience < now3 : 
                print("ya existe el id y la fecha " + date_audience.strftime('%Y-%m-%dT%H:%M:%S+%f')+ " esta cerca a " + now3.strftime('%Y-%m-%dT%H:%M:%S+%f')) 
    
    #si no existe audience id va a insertar datos iniciales
    elif mydb.reports.count_documents({ 'audienceId': a['id'] }, limit = 1) == 0 :
                    reportsColl.insert_one({
                            'audienceId': a['id'],
                            'clientId': partner_id,
                            'audienceName': a['name'],
                            # 'audienceCreationDate': dateAudienceCreate,
                            # 'audienceModifiedDate': date_audience,
                            # 'audienceCountryCodes': a['countryCodes'],
                            # 'audienceDeviceType': a['device_type'],
                            # 'clientId': partner_id,
                            # "recency" : a['recency'],
                            # 'typePlatform': 3,
                            # 'namePlatform': 'bluekai',
                            # 'segment':'',
                            'discovery':{"categoriesId":""}
                    })
                    print("inserta reports", a['id'])
    #si existe id de audiencia y el date de modificacion esta lejos
    elif mydb.reports.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience > now3 :
    
                    reportsColl.insert_one({
                        # print(a['id']) 
                        # print(a['name']) 
                        # print(a['updated_at'])
                        # print(a['countryCodes'])
                        # print(a['idTypes'])
                        # print(a['recency'])
                        # print("segments", a['segments'])
                        # print("status", a['status'])
    
                            'audienceId': a['id'],
                            'clientId': partner_id,
                            'audienceName': a['name'],
                            # 'audienceCreationDate': dateAudienceCreate,
                            # 'audienceModifiedDate': date_audience,
                            # 'audienceCountryCodes': a['countryCodes'],
                            # 'audienceDeviceType': a['device_type'],
                            # 'clientId': partner_id,
                            # "recency" : a['recency'],
                            # 'typePlatform': 3,
                            # 'namePlatform': 'bluekai',
                            # 'segment':'', 
                            'discovery':{"discoveryId":""}                         
                    })
                    print("actualiza reports", a['id']) 
# DF = pd.DataFrame({"Name": audience_names , "ID":audience_id})
# print("DF", DF)

# Name = []
# Reach = []
# Status = []
# Details = []
# Price = []
# Ids = []


    cont = 1
    a = AUD.DetailAudience(partner_id, str(aud_id), bkuid, bksecretkey)
    segments = a['segments'];
    segmentBody = segments

# segmentBody = segmentBody.replace(" ", "")
# escapeJSON = function(str) {
#     return str.replace(/\\/g,'\\');
# };

# segmentBody = segmentBody.escapeJSON();
# # u = urlopen(segments)
# rawData = u.read()
# with open('data.json', 'w', encoding='utf-8') as f:
#     f.write(rawData.decode('utf-8'))
    print ("audienceID", audience_id)
    for aud_id in audience_id:
        # hint = str(cont)+"\r"
        # print("hint",hint)
        data = segments
        # print("fruits",data)
        # print("data para insertar", aud_id)
        # if mydb.segments.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience < now3 : 
        #         print("ya existe el id y la fecha " + date_audience.strftime('%Y-%m-%dT%H:%M:%S+%f')+ " es menor a " + now2.strftime('%Y-%m-%dT%H:%M:%S+%f')) 
        
        #actualiza el segmento si esta el id de la audiencia
        if mydb.audiences.count_documents({ 'audienceId': aud_id }, limit = 1):
                    # segmentColl.update_one({
                    #         # 'audienceId': a['id'],
                    #         # 'audienceName': a['name'],
                    #         # 'audienceCreationDate': dateAudienceCreate,
                    #         # 'audienceModifiedDate': date_audience,
                    #         data
                    # })
                    # print (audienceColl.update_one({ 'audienceId': audience_id },{"$set": {
                    #             'segment': data
                    #                 }}, upsert=False))
                    
                    #actualiza segmento en audiences
                    audienceColl.update_one({'audienceId': aud_id },{"$set": {
                                'segment': data
                                    }}, upsert=False)
                    
                    print("actualizo segmento")
        # elif mydb.segments.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience > now3 :
        #             audienceColl.insert_one({
        #                     'audienceId': a['id'],
        #                     'audienceName': a['name'],
        #                     'audienceCreationDate': dateAudienceCreate,
        #                     'audienceModifiedDate': date_audience,
        #                     'audienceCountryCodes': a['countryCodes'],
        #                     'audienceDeviceType': a['device_type'],
        #                     'clientId': partner_id,
        #                     "recency" : a['recency'],
        #                     'typePlatform': 3,
        #                     'namePlatform': 'bluekai'
        #             })
        #             print("actualiza ", a['id']) 
        # print(data)
        # orig_stdout = sys.stdout
        # f = open('llisting.log', 'w')
        # sys.stdout = f
        # #  print("va a entrar")
        
        # Details.append(d)
        # Ids.append(aud_id)
        # print("data list -->\n",segmentBody)
        r = AUD.ReachAudience(partner_id, str(aud_id), bkuid, bksecretkey, segmentBody)
        reach = r;
        # print ("reach -->\n", reach)
        
        #actualiza reach en audiences si esta el id de audiencia
        if mydb.audiences.count_documents({ 'audienceId': aud_id }, limit = 1):

                    audienceColl.update_one({'audienceId': aud_id },{"$set": {
                                'reach': reach
                                    }}, upsert=False)
                    
                    print("actualizo reach")
        
        d = AUD.DiscoveryAudience(partner_id, str(aud_id), bkuid, bksecretkey, reach)
        discovery = d;
        # print("discovery---->",discovery)
        #verifica si hay alguna fecha nueva para discoverys
        report_one = reportsColl.find_one({"date": discovery['date']})
        # print("existe fecha ?",report_one) 
        if mydb.reports.count_documents({ 'audienceId': aud_id }, limit = 1) and report_one is not None:                   
                    print("si hay date")             
                    #arma el contenido de categoriesids en discovery 
                    for categoriesDisc in discovery['Audiences']['1']:

                        categorieDiscmongo.append({
                                    "category_id":categoriesDisc['categoryId'],            
                                    "second_segment_reach":categoriesDisc['backgroundSegmentSize'],
                                    "category_index":categoriesDisc['segment1Index'],
                                    "base_segment_size_filtered":categoriesDisc['segment1Size'],
                                    "internal_leftCI":categoriesDisc['segment1LeftCI'],
                                    "internal_rightCI":categoriesDisc['segment1RightCI'],
                                    "internal_CL":categoriesDisc['segment1CL']
                        })
                    #actualiza el objeto de discovery
                    print("categoriemongo",categorieDiscmongo)
                    reportsColl.update_one({'audienceId': aud_id },{"$set": {
                                'base_segment_size_unfiltered': discovery['totalsegment1Size'],
                                'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
                                'date': discovery['date'],
                                'discovery': categorieDiscmongo
                                
                    }}, upsert=False)
                    
                    print("actualizo discovery date en reports")

        elif mydb.reports.count_documents({ 'audienceId': aud_id }, limit = 1) and report_one is None:
                    print("no hay fecha y crea un nuevo doc")
                    for categoriesDisc in discovery['Audiences']['1']:
                        categorieDiscmongo.append([{
                                "category_id":categoriesDisc['categoryId'],                               
                                "second_segment_reach":categoriesDisc['backgroundSegmentSize'],
                                "category_index":categoriesDisc['segment1Index'],
                                "base_segment_size_filtered":categoriesDisc['segment1Size'],
                                "internal_leftCI":categoriesDisc['segment1LeftCI'],
                                "internal_rightCI":categoriesDisc['segment1RightCI'],
                                "internal_CL":categoriesDisc['segment1CL']
                        }])
                                                        
                    reportsColl.insert_one({
                            'audienceId': a['id'],
                            'clientId': partner_id,
                            'audienceName': a['name'],
                            'base_segment_size_unfiltered': discovery['totalsegment1Size'],
                            'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
                            'date': discovery['date'],
                            'discovery': categorieDiscmongo
                    })
                    print("inserta reports con nueva fecha", a['id'])
 
    c = AUD.CategoryAudience(partner_id, bkuid, bksecretkey)
    categorieDB = c
    print("categories------->",categorieDB)
    for categorieItem in categorieDB['items']:
 
                    categoriesColl.insert_one({
                        "category_id":categorieItem['id'],
                        "category_name":categorieItem['name'],
                        # "category_description":categorieItem['description'],
                        "partner_id":categorieItem['partner']['id'],
                        "vertical_name":categorieItem['vertical']['name'],
                        "data_type":categorieItem['ownershipType'],
                        "path_array":categorieItem['pathFromRoot']
                    })
                    print("insertando", categorieItem['name'])                       
