from configparser import ConfigParser
# from pymongo.errors import BulkWriteError
# import pymongo
import BluekaiAudienceConnect as bk #Traducción by Hexagon
from datetime import datetime, timedelta, date


bkuid = '72c487d4f0318706ce96a0ebe47b906342cb0a2e3610fe182746602c3be6d016' #Web Service User Key
bksecretkey = '90388366ed94cfaea8ee163e0c64200a587974ce7492ec827b33e84ba644bad0' #Web Service Private Key
# url = 'http://services.bluekai.com/Services/WS/Ping'
#pid = '3740'
Url = "https://services.bluekai.com/Services/WS/audiences?pid=4355"
partner_id = "3740"
aud_id= "581494"
    # parser = ConfigParser()    
    # parser.read('config.cfg')
    # bkuid = parser.get('api_samples_bluekai', 'bkuid')
    # bksecretkey = parser.get('api_samples_bluekai', 'bksecretkey')
    # pid = parser.get('api_samples_bluekai', 'pid')
    # DB_USER = parser.get('DB', 'DB_USER')
    # DB_PASSWORD = parser.get('DB', 'DB_PASSWORD')
    # NODE_ENV = parser.get('ENVIRONMENT', 'NODE_ENV')
def getAudience():
    AUD = bk.BluekaiAudienceCall(Url, "GET")
    JsonList = AUD.ListAudience(partner_id, bkuid, bksecretkey)
    audiences = JsonList["audiences"]
    print(audiences)

#     JsonList = AUD.DetailAudience(partner_id, str(aud_id), bkuid, bksecretkey)
#     audiences = JsonList["audiences"]
#     print(audiences)


#     parser = ConfigParser()
#     parser.read('config.cfg')
#     DB_USER = parser.get('DB', 'DB_USER')
#     DB_PASSWORD = parser.get('DB', 'DB_PASSWORD')
#     NODE_ENV = parser.get('ENVIRONMENT', 'NODE_ENV')
    #print(audiences[0])
#     if NODE_ENV == 'production' :
#             myclient = pymongo.MongoClient('mongodb+srv://'+ DB_USER +':'+ DB_PASSWORD +'@audiencekit1-76o4f.mongodb.net/audienceKit?retryWrites=true&w=majority')
#             mydb = myclient["audienceKit"]
#             mycol = mydb["segments"]
#     elif NODE_ENV == 'development' :
#             myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#             mydb = myclient["audienceKit"]
#             mycol = mydb["segments"]

#     for i in audiences:
#             #2020-04-22T15:20:17-0500
#             date_audience = datetime.strptime(i['updated_at'],'%Y-%m-%dT%H:%M:%S-%f')
#             dateAudienceCreate = datetime.strptime(i['created_at'],'%Y-%m-%dT%H:%M:%S-%f')
#             now = datetime.now()
#             now2 = datetime.today()
#             now3 = now2 - timedelta(days=6000)
#             #print(i) 
#             # print(i['id']) 
#             # print(i['name']) 
#             # print(i['updated_at'])
#             # print(i['countryCodes'])
#             # print(i['idTypes'])
#             # print(i['recency'])
#             # print("segments", i['segments'])
#             # print("status", i['status'])
#             # with open("ping.txt", "w") as f:
#             #f.write(str(audiences))
#             if mydb.segments.count_documents({ 'audienceId': i['id'] }, limit = 1) and date_audience < now3 : 
#                 print("ya existe el id y la fecha " + date_audience.strftime('%Y-%m-%dT%H:%M:%S-%f')+ " es menor a " + now2.strftime('%Y-%m-%dT%H:%M:%S-%f')) 

#             elif mydb.segments.count_documents({ 'audienceId': i['id'] }, limit = 1) == 0 :
#                     mycol.insert_one({
#                     # print(i['id']) 
#                     # print(i['name']) 
#                     # print(i['updated_at'])
#                     # print(i['countryCodes'])
#                     # print(i['idTypes'])
#                     # print(i['recency'])
#                     # print("segments", i['segments'])
#                     # print("status", i['status'])
                    
#                                         'audienceId': i['id'],
#                                         'audienceName': i['name'],
#                                         #viene de un formulario del usuario
#                                         "displayName":None,
#                                         'audienceDescription': None,
#                                         'audienceCreationDate': dateAudienceCreate,
#                                         'audienceModifiedDate': date_audience,
#                                         'audienceRules': i['segments'],
#                                         'audienceType': i['idTypes'],
#                                         'clientId': partner_id,
#                                         'clientName': None,
#                                         'monthlyUniques': None,
#                                         "reach" : i['reach'],
#                                         "recency" : i['recency'],
#                                         "images": None,
#                                         "category":None,
#                                         'keywords': None,
#                                         'typePlatform': 3,
#                                         'namePlatform': 'bluekai',
#                                         # viene de formulaqio de usuario
#                                         'urlIframe': None,
#                                         'status': 0
#                                             })
#                     print("inserta", i['id']) 
                    
#             # existe y la audiencia es mayor
            
#             elif mydb.segments.count_documents({ 'audienceId': i['id'] }, limit = 1) and date_audience > now3 :

#                     mycol.insert_one({
#                     # print(i['id']) 
#                     # print(i['name']) 
#                     # print(i['updated_at'])
#                     # print(i['countryCodes'])
#                     # print(i['idTypes'])
#                     # print(i['recency'])
#                     # print("segments", i['segments'])
#                     # print("status", i['status'])
                    
#                                         'audienceId': i['id'],
#                                         'audienceName': i['name'],
#                                         #viene de un formulario del usuario
#                                         "displayName":None,
#                                         'audienceDescription': None,
#                                         'audienceCreationDate': dateAudienceCreate,
#                                         'audienceModifiedDate': date_audience,
#                                         'audienceRules': i['segments'],
#                                         'audienceType': i['idTypes'],
#                                         'clientId': partner_id,
#                                         'clientName': None,
#                                         'monthlyUniques': None,
#                                         "reach" : i['reach'],
#                                         "recency" : i['recency'],
#                                         "images": None,
#                                         "category":None,
#                                         'keywords': None,
#                                         'typePlatform': 3,
#                                         'namePlatform': 'bluekai',
#                                         # viene de formulaqio de usuario
#                                         'urlIframe': None,
#                                         'status': 0
#                                             })
#                     print("actualiza ", i['id']) 


getAudience()