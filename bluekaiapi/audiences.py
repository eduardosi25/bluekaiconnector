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
import dateutil
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

# print ("\nResponse Code: 200")

audiences = JsonList["audiences"]
# print(audiences)
audience_names = []
audience_id = []
category = []
# categorieDiscmongo = []
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
resultCategory = categoriesColl.find()




'''recorre audiencias para sacar audience ids'''
for a in audiences:
    # json_formatted_str = json.dumps(a, indent=2)
    # print("audiencias--->",json_formatted_str)
    # audience_names.append(a["name"])
    audience_id.append(str(a["id"]))
    date_audience = datetime.strptime(a['updated_at'],'%Y-%m-%dT%H:%M:%S+%f')
    dateAudienceCreate = datetime.strptime(a['created_at'],'%Y-%m-%dT%H:%M:%S+%f')
    now = datetime.now()
    now2 = datetime.today()
    # now3 = now2 - timedelta(days=6000)
    now3 = now2 - timedelta()
    print("id ------->",a['id'])
    # audID = str(a['id'])

    if mydb.audiences.count_documents({ 'audienceId': str(a['id']) }, limit = 1) and date_audience < now3 : 
        print("ya existe el id y la fecha " + date_audience.strftime('%Y-%m-%dT%H:%M:%S+%f')+ " es menor a " + now2.strftime('%Y-%m-%dT%H:%M:%S+%f')) 
    elif mydb.audiences.count_documents({ 'audienceId': str(a['id']) }, limit = 1) == 0 :
                    audienceColl.insert_one({
                            'audienceId': str(a['id']),
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
                            'audienceId': str(a['id']),
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
                    print("inserta audiencias", str(a['id'])) 

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
        #actualiza el segmento si esta el id de la audiencia
        if mydb.audiences.count_documents({ 'audienceId': aud_id }, limit = 1):
                    #actualiza segmento en audiences
                    audienceColl.update_one({'audienceId': aud_id },{"$set": {
                                'segment': data
                                    }}, upsert=False)
                    
                    print("actualizo segmento")
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
        # json_formatted_str = json.dumps(discovery, indent=2)
        # print("DISCOVERY--->",json_formatted_str)
        # print("discovery---->",discovery)
        #verifica si hay alguna fecha nueva para discoverys
        global report_date
 
        report_date = discovery['date']
        print("report_date",report_date)
        # json_formatted_str = json.dumps(a, indent=2)
        # print("audiencias--->",json_formatted_str)
 

#si no existe audience id va a insertar datos iniciales
        if mydb.reports.count_documents({ 'audienceId': str(a['id']) }, limit = 1) == 0:
            for categoriesDisc in discovery['Audiences']['1']:

                category_id = categoriesDisc['categoryId']
                second_segment_reach = categoriesDisc['backgroundSegmentSize']
                category_index = categoriesDisc['segment1Index']
                base_segment_size_filtered = categoriesDisc['segment1Size']
                internal_leftCI = categoriesDisc['segment1LeftCI']
                internal_rightCI = categoriesDisc['segment1RightCI']
                internal_CL = categoriesDisc['segment1CL']
                
                reportsColl.insert_one({
                        'audienceId': str(a['id']),
                        'clientId': partner_id,
                        'audienceName': a['name'],
                        'base_segment_size_unfiltered': discovery['totalsegment1Size'],
                        'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
                        'date': dateutil.parser.parse(discovery['date']),
                        'category_id': category_id,
                        'second_segment_reach': second_segment_reach,
                        'category_index': category_index,
                        'base_segment_size_filtered': base_segment_size_filtered,
                        'internal_leftCI': internal_leftCI,
                        'internal_rightCI': internal_rightCI,
                        'internal_CL': internal_CL     
                })
                print("inserta datos iniciales", category_id)



#si audience id existe y ya hay un date en report cerca
        elif mydb.reports.count_documents({ 'audienceId': aud_id }, limit = 1) and date_audience < now3 :                   
            # print("si hay date cerca")             
                    #arma el contenido de categoriesids en reports
            for categoriesDisc in discovery['Audiences']['1']:
                categoryIdInR = reportsColl.find_one({"category_id": categoriesDisc['categoryId']})

                category_id = categoriesDisc['categoryId']
                second_segment_reach = categoriesDisc['backgroundSegmentSize']
                category_index = categoriesDisc['segment1Index']
                base_segment_size_filtered = categoriesDisc['segment1Size']
                internal_leftCI = categoriesDisc['segment1LeftCI']
                internal_rightCI = categoriesDisc['segment1RightCI']
                internal_CL = categoriesDisc['segment1CL']

# #compara categoria en categories y categoria en report para ver si esta ... cual id esta ??
#                 if categoryIdInR is not None:
#                 #ya esta en base y actualiza el incompleto
#                         print("categoryIdInR---->",categoryIdInR['_id'])

#                         reportsColl.update_one({'_id': categoryIdInR['_id'] },{"$set": {
#                                             'base_segment_size_unfiltered': discovery['totalsegment1Size'],
#                                             'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
#                                             'second_segment_reach': discovery['date'],
#                                             'category_id': category_id,
#                                             'second_segment_reach': second_segment_reach,
#                                             'category_index': category_index,
#                                             'base_segment_size_filtered': base_segment_size_filtered,
#                                             'internal_leftCI': internal_leftCI,
#                                             'internal_rightCI': internal_rightCI,
#                                             'internal_CL ': internal_CL         
#                         }}, upsert=False)
#                         print("actualizo discovery date en reports")
                
                if categoryIdInR is None:
                        
                        reportsColl.insert_one({
                                'audienceId': str(a['id']),
                                'clientId': partner_id,
                                'audienceName': a['name'],
                                'base_segment_size_unfiltered': discovery['totalsegment1Size'],
                                'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
                                'date': dateutil.parser.parse(discovery['date']),
                                'category_id': category_id,
                                'second_segment_reach': second_segment_reach,
                                'category_index': category_index,
                                'base_segment_size_filtered': base_segment_size_filtered,
                                'internal_leftCI': internal_leftCI,
                                'internal_rightCI': internal_rightCI,
                                'internal_CL': internal_CL     
                        })
                        print("inserta nuevo document_report por nuevo category id", category_id)
                        
                        # reportsColl.update_one({'_id': categoryIdInR['_id'] },{"$set": {
                        #                     'base_segment_size_unfiltered': discovery['totalsegment1Size'],
                        #                     'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
                        #                     'second_segment_reach': discovery['date'],
                        #                     'category_id': category_id,
                        #                     'second_segment_reach': second_segment_reach,
                        #                     'category_index': category_index,
                        #                     'base_segment_size_filtered': base_segment_size_filtered,
                        #                     'internal_leftCI': internal_leftCI,
                        #                     'internal_rightCI': internal_rightCI,
                        #                     'internal_CL ': internal_CL         
                        # }}, upsert=False)

# si audience id existe y date esta lejos
        elif mydb.reports.count_documents({ 'audienceId': aud_id }, limit = 1) and date_audience > now3:
            print("la fecha es vieja y crea un nuevo doc")
            
            for categoriesDisc in discovery['Audiences']['1']:
 
                category_id = categoriesDisc['categoryId']
                second_segment_reach = categoriesDisc['backgroundSegmentSize']
                category_index = categoriesDisc['segment1Index']
                base_segment_size_filtered = categoriesDisc['segment1Size']
                internal_leftCI = categoriesDisc['segment1LeftCI']
                internal_rightCI = categoriesDisc['segment1RightCI']
                internal_CL = categoriesDisc['segment1CL']


                reportsColl.insert_one({
                        'audienceId': str(a['id']),
                        'clientId': partner_id,
                        'audienceName': a['name'],
                        'base_segment_size_unfiltered': discovery['totalsegment1Size'],
                        'odc_universe_reach': discovery['totalbackgroundSegmentSize'],
                        'date': dateutil.parser.parse(discovery['date']),
                        'category_id': category_id,
                        'second_segment_reach': second_segment_reach,
                        'category_index': category_index,
                        'base_segment_size_filtered': base_segment_size_filtered,
                        'internal_leftCI': internal_leftCI,
                        'internal_rightCI': internal_rightCI,
                        'internal_CL': internal_CL     
                })
                print("inserta reports con nueva fecha", a['id'])


#inyecta en categories


    # c = AUD.CategoryAudience(partner_id, bkuid, bksecretkey)
    # categorieDB = c['items']
    # # print("categories------->",categorieDB)
    
    # for categorieItem in categorieDB:
    #         categoryIdExist = categoriesColl.find_one({"category_id": categorieItem['id']})
            
    #         # description =  'description' in categorieItem
    #         # print(description)
    #         # description1 =  categorieItem['description'] in categorieItem
    #         # print(description1)
    #         if 'description' in categorieItem:

    #             if categoryIdExist is None:
    #                 categoriesColl.insert_one({
    #                 "category_id":categorieItem['id'],
    #                 "category_name":categorieItem['name'],
    #                 "category_description":categorieItem['description'],
    #                 "partner_id":categorieItem['partner']['id'],
    #                 "vertical_name":categorieItem['vertical']['name'],
    #                 "data_type":categorieItem['ownershipType'],
    #                 "path_array":categorieItem['pathFromRoot']
    #                 })
    #             #     print("insertando", categorieItem['name'])                       
    #             # else:
    #             #     print("ya existe id", categorieItem['id'])
    #         else:
    #             if categoryIdExist is None:
                    
    #                 categoriesColl.insert_one({
    #                 "category_id":categorieItem['id'],
    #                 "category_name":categorieItem['name'],
    #                 "partner_id":categorieItem['partner']['id'],
    #                 "vertical_name":categorieItem['vertical']['name'],
    #                 "data_type":categorieItem['ownershipType'],
    #                 "path_array":categorieItem['pathFromRoot']
    #                 })
    #             #     print("insertando sin descripcion", categorieItem['name'])                       
    #             # else:
    #             #     print("ya existe id sin descripcion", categorieItem['id'])




            # reportMatch = categoriesColl.find_one({"category_id": categorieItem['id']})
            # print(reportMatch["category_id"])


    resultReport = reportsColl.find()
#inyecta en reports datos de categorias

    for r in resultReport:

#tiene que verificar para el date de discovery
        report_date2 = reportsColl.find_one({"date": r['date']})
        print("report_date", report_date)
        print("report_date2", report_date2['date'])
        if report_date == report_date2 or report_date is not None:
        
            if 'category_name' not in r:      
# si no hay category name no ha escrito esta parte         
                # print(match)
                # print(match['category_id'])
                # revisa en categories si existe el id de category
                reportIdExist = categoriesColl.find_one({"category_id": r['category_id']})
                # print(reportIdExist)

    # comprueba si el category_id esta en el catalogo de categories
                if reportIdExist is None:
                    print("no existe en categories", r['category_id'])
                else:
                    # print("el id coincidió ", reportIdExist['category_id'])
                    # print("names path -->",reportIdExist['path_array']['names'])

                        namesPath = ''
                        l=len(reportIdExist['path_array']['names'])-1 
                        # print("namesindex --> ", l)
                        i=0
                        for value in reportIdExist['path_array']['names']:
                            # print("i-->",i)
            #compara index para quitar la flecha y quita root
                            if l==i:
                                if value != 'ROOT':
                                # print("path---->",value)
                                    namesPath+=value
                            else:
                                if value != 'ROOT':
                                    # print("path---->",value)
                                    namesPath+=value+" -> "
                            i= i+1
                        # print("namespath -->",namesPath)
                        if 'category_description' in reportIdExist:
                            reportsColl.update_one({'category_id': reportIdExist['category_id'] },{"$set": {
                                    "category_name":reportIdExist['category_name'],                 
                                    "category_description":reportIdExist['category_description'],
                                    "vertical_name":reportIdExist['vertical_name'],
                                    "data_type":reportIdExist['data_type'],
                                    "path_array":namesPath
                                    }}, upsert=False)
                            print("actualizo a reports con descripcion", reportIdExist['category_id'])
                        else:
                            reportsColl.update_one({'category_id': reportIdExist['category_id'] },{"$set": {
                                
                                    "category_name":reportIdExist['category_name'],
                                    "vertical_name":reportIdExist['vertical_name'],
                                    "data_type":reportIdExist['data_type'],
                                    "path_array":namesPath
                                    }}, upsert=False)
                            print("actualizo a reports sin descripcion", reportIdExist['category_id'])

            else:
                print("ya existe category_name en mongo no es necesario actualizar")
        else: 
            print("no hay fecha que coincida o es nula")
