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
categoriemongo = []
categorieInsert = []
# if NODE_ENV == 'production' :
#         myclient = pymongo.MongoClient('mongodb+srv://'+ DB_USER +':'+ DB_PASSWORD +'@audiencekit1-76o4f.mongodb.net/audienceKit?retryWrites=true&w=majority')
#         mydb = myclient["bluekaiconnector"]
#         audienceColl = mydb["audiences"]
# elif NODE_ENV == 'development' :
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["bluekaiconnector"]
audienceColl = mydb["audiences"]
reportsColl = mydb["reports"]
categoriesColl = mydb["categories"]

'''
recorre audiencias para sacar audience id
'''
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
                            'reach':'',
                            'discovery':''
                    })
                    print("inserta audiencias", a['id'])
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
                            'reach':'',
                            'discovery':''                         
                    })
                    print("actualiza audiencias", a['id']) 


    if mydb.reports.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience < now3 : 
                print("ya existe el id y la fecha " + date_audience.strftime('%Y-%m-%dT%H:%M:%S+%f')+ " es menor a " + now2.strftime('%Y-%m-%dT%H:%M:%S+%f')) 
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
                # reach = str(reach)
                audienceColl.update_one({'audienceId': aud_id },{"$set": {
                            'reach': reach
                                }}, upsert=False)
                
                print("actualizo reach")
    
    d = AUD.DiscoveryAudience(partner_id, str(aud_id), bkuid, bksecretkey, reach)
    discovery = d;
    # print ("discovery -->", discovery['Audiences'])
    # discovery = str(discovery)
    # if mydb.audiences.count_documents({ 'audienceId': aud_id }, limit = 1):
    #             # segmentColl.update_one({
    #             #         # 'audienceId': a['id'],
    #             #         # 'audienceName': a['name'],
    #             #         # 'audienceCreationDate': dateAudienceCreate,
    #             #         # 'audienceModifiedDate': date_audience,
    #             #         data
    #             # })
    #             # print (audienceColl.update_one({ 'audienceId': audience_id },{"$set": {
    #             #             'segment': data
    #             #                 }}, upsert=False))
    #             audienceColl.update_one({'audienceId': aud_id },{"$set": {
    #                         'discovery': discovery
    #                             }}, upsert=False)
                
    #             print("actualizo discovery en audiencias")
    
    if mydb.reports.count_documents({ 'audienceId': aud_id }, limit = 1):
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
      
      
      

 
 
    
                conta = 0
            
                for categories in discovery['Audiences']['1']:
                    hint = str(conta)
                    categoriemongo.append([{"category_id":categories['categoryId'],
                                            
                                            "second_segment_reach":categories['backgroundSegmentSize'],
                                            "category_index":categories['segment1Index'],
                                            "base_segment_size_filtered":categories['segment1Size'],
                                            "internal_leftCI":categories['segment1LeftCI'],
                                            "internal_rightCI":categories['segment1RightCI'],
                                            "internal_CL":categories['segment1CL']
                                            }])
                    # print("hint",hint)
                    
                    # reportsColl.update_one({'audienceId': aud_id },{"$set": {
                    #         'discovery': {
                    #                         hint : {
                    #                             'category_id': categories['categoryId']
                    #                             }
                    #                     }
                    #             }}, upsert=False)
                    # print("inserto categorias id", hint)
                    conta = conta + 1
                # category.append(categories["categoryId"])
                # print(categoriemongo)
                
                reportsColl.update_one({'audienceId': aud_id },{"$set": {
                            'base_segment_size_unfiltered': discovery['totalsegment1Size'],
                            'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
                            'date': discovery['date'],
                            'discovery': {
                                            
                                            'categoriesId': categoriemongo,
                                            
                                        }
                            
                                }}, upsert=False)
                
                print("actualizo discovery date en reports")
                # reportsColl.update_one({'audienceId': aud_id },{"$set": {
                #             'discoveryID': {
                #                             categoriemongo
                #                         }
                #                 }}, upsert=False)
    # if mydb.audiences.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience < now3 : 
    #             print("ya existe el id y la fecha " + date_audience.strftime('%Y-%m-%dT%H:%M:%S+%f')+ " es menor a " + now2.strftime('%Y-%m-%dT%H:%M:%S+%f')) 
    # elif mydb.audiences.count_documents({ 'audienceId': a['id'] }, limit = 1) == 0 :
    #                 audienceColl.insert_one({
    #                         'audienceId': a['id'],
    #                         'audienceName': a['name'],
    #                         'audienceCreationDate': dateAudienceCreate,
    #                         'audienceModifiedDate': date_audience,
    #                         'audienceCountryCodes': a['countryCodes'],
    #                         'audienceDeviceType': a['device_type'],
    #                         'clientId': partner_id,
    #                         "recency" : a['recency'],
    #                         'typePlatform': 3,
    #                         'namePlatform': 'bluekai',
    #                         'segment':'',
    #                         'reach':'',
    #                         'discovery':''
    #                 })
    #                 print("inserta", a['id'])
    # elif mydb.audiences.count_documents({ 'audienceId': a['id'] }, limit = 1) and date_audience > now3 :
    
    #                 audienceColl.insert_one({
    #                     # print(a['id']) 
    #                     # print(a['name']) 
    #                     # print(a['updated_at'])
    #                     # print(a['countryCodes'])
    #                     # print(a['idTypes'])
    #                     # print(a['recency'])
    #                     # print("segments", a['segments'])
    #                     # print("status", a['status'])
    
    #                         'audienceId': a['id'],
    #                         'audienceName': a['name'],
    #                         'audienceCreationDate': dateAudienceCreate,
    #                         'audienceModifiedDate': date_audience,
    #                         'audienceCountryCodes': a['countryCodes'],
    #                         'audienceDeviceType': a['device_type'],
    #                         'clientId': partner_id,
    #                         "recency" : a['recency'],
    #                         'typePlatform': 3,
    #                         'namePlatform': 'bluekai',
    #                         'segment':'', 
    #                         'reach':'',
    #                         'discovery':''                         
    #                 })
    #                 print("actualiza ", a['id']) 
                # for categories in discovery['Audiences']['1']:
                

c = AUD.CategoryAudience(partner_id, bkuid, bksecretkey)
categorieDB = c
                # categories = categories.replace(" ", "")
                # categories = categories.replace("\'", "\"")
                # categorieDB = categories.encode('utf-8')
                # print (rawData.decode("UTF-8"))
                # print ("categories -->", categorieDB["items"])
# with open('datas.json', 'w', encoding='utf-8') as f:
#     f.write(str(categorieDB)) 
                # print ("categories",categories)
categoriecont = 0
for categorieItem in categorieDB['items']:
                    hintcat = str(categoriecont)
                    # print("contador \n", hintcat)
        #  if mydb.categories.count_documents({ 'audienceId': aud_id }, limit = 1):
               
                    # categorieInsert.append({"category_name":categorieItem['name'],
                    #                         # "category_description":categorieItem['description'],
                    #                         "partner_id":categorieItem['partner']['id'],
                    #                         "vertical_name":categorieItem['vertical']['name'],
                    #                         "data_type":categorieItem['ownershipType'],
                    #                         "path_array":categorieItem['pathFromRoot']
                    # })
                    # categoriecont = categoriecont + 1                    
                    
                    # print("insertar", categorieInsert)
                    # mydata = input('Prompt :')
                    
                    # print("para actualizar", type(categorieInsert))     


                    # print("tipo ",type(categorieInsert))
                    # if mydb.categories.count_documents({ 'vertical_name': categorieItem['vertical']['name'] }, limit = 1):
                    # #     categoriesColl.insert_one({"category_name":categorieItem['name'],
                    # #                         # "category_description":categorieItem['description'],
                    # #                         "partner_id":categorieItem['partner']['id'],
                    # #                         "vertical_name":categorieItem['vertical']['name'],
                    # #                         "data_type":categorieItem['ownershipType'],
                    # #                         "path_array":categorieItem['pathFromRoot']
                    # # })
                    #         print("ya esta en la base", categorieItem['vertical']['name'])
                    # # si no esta en la base inserta
                    # elif mydb.categories.count_documents({ 'vertical_name': categorieItem['vertical']['name'] }, limit = 1) == 0 :                
                    categoriesColl.insert_one({"category_name":categorieItem['name'],
                                            # "category_description":categorieItem['description'],
                                            "partner_id":categorieItem['partner']['id'],
                                            "vertical_name":categorieItem['vertical']['name'],
                                            "data_type":categorieItem['ownershipType'],
                                            "path_array":categorieItem['pathFromRoot']
                    })
                    print("insertando")                       

# print ("categories",categoriesInsert)
# categorieInsert = str(categorieInsert)
# categorieInsert =categorieInsert.replace("\'", "\"")
    # with open('data.json', 'w', encoding='utf-8') as f:
    #     f.write(str(categorieInsert)) 
# categorieInsert = categorieInsert.encode('utf-8')





cont = cont + 1
    