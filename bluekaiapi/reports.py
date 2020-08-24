import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["bluekaiconnector"]
audienceColl = mydb["audiences"]
reportsColl = mydb["reports"]
categoriesColl = mydb["categories"]

resultCategory = categoriesColl.find()
resultReport = reportsColl.find()

i=0

# for match in reportsColl.find({ "discovery.0.category_id" : { "$in" : [123] } }):
#     print(match)
for r in resultReport:
    for match in r['discovery']:
        # print(match)
        # print(match['category_id'])
        # revisa en categories si existe el id de category
        reportIdExist = categoriesColl.find_one({"category_id": match['category_id']})
        print(reportIdExist)
        if reportIdExist is None:
            print("no existe en categories", match['category_id'])
        else:
            # print("el id coincidió ", reportIdExist['category_id'])
    
            if 'category_description' in reportIdExist:
                reportsColl.update_one({'discovery.category_id': reportIdExist['category_id'] },{"$set": {
                        "discovery.$.category_name":reportIdExist['category_name'],                 
                        "discovery.$.category_description":reportIdExist['category_description'],
                        "discovery.$.vertical_name":reportIdExist['vertical_name'],
                        "discovery.$.data_type":reportIdExist['data_type'],
                        "discovery.$.path_array":reportIdExist['path_array']
                        }}, upsert=False)
                print("actualizo a reports", reportIdExist['category_id'])
            else:
                reportsColl.update_one({'discovery.category_id': reportIdExist['category_id'] },{"$set": {
                    
                        "discovery.$.category_name":reportIdExist['category_name'],
                        "discovery.$.vertical_name":reportIdExist['vertical_name'],
                        "discovery.$.data_type":reportIdExist['data_type'],
                        "discovery.$.path_array":reportIdExist['path_array']
                        }}, upsert=False)
                print("actualizo a reports sin descripcion", reportIdExist['category_id'])
    print(i)
    i= i+1

