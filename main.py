import pymongo
import pandas as pd
import time
import random
import string
import ast

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["movies"]
char_set = string.ascii_uppercase + string.digits

def randString():
    return ''.join(random.sample(char_set*35, 35))

def insertData(data, name):
    t0 = time.time()
    col = db[name]
    col.drop()
    x = col.insert_many(data)
    t1 = time.time()
    print(f"inserted {len(x.inserted_ids)} records of {name} in {round(t1-t0, 2)}s")

def insertCSV(csv, name, jsonCols = []):
    t0 = time.time()
    data = pd.read_csv(csv, dtype=str).to_dict('records')
    for document in data:
        for col in jsonCols:
            if(type(document[col]) == str): document[col] = ast.literal_eval(document[col])
    t1 = time.time()
    print(f"read {name} in {round(t1-t0, 2)}s")
    insertData(data, name)

#DONE: Pronadji sve kljucne reci koje se pojavljuju kod filmova odredjenog zanra, i poredjaj ih od najcescih do najredjih
def upit1_1_1():
    db['genres_with_keywords'].drop()
    db['metadata'].aggregate([
        { '$unwind': '$genres' },
        { '$match': { 'genres.name': 'Drama'} },
        { '$project': {'genres': 1, 'original_title': 1, 'id': 1 } },
        { "$lookup": 
           {
                "from": "keywords",
                "localField": "id",
                "foreignField": "id",
                "pipeline":
                [
                    #{ '$project': {"_id": 0} },
                    { '$unwind': '$keywords' }
                ],
                "as": "keywordsForGenres"
            }
        },
        { '$unwind': '$keywordsForGenres' },
        # , # tresem se i placem evo sat vremena smo resavali problem kog nije bilo jaoj majko
        { "$sortByCount" : "$keywordsForGenres.keywords.name" }, #todorova magija
        { '$merge': 'genres_with_keywords' }
    ])

#DONE: Za prvih 5 glumaca iz tabele 'credits_per_actor' pronadji sve jezike koji su se govorili u svim filmovima u kojima su oni glumili
# Hteo sam da napravim tako da bukvalno uzimam prvih 5 dokumenata iz tabele 'credits_per_actor' i da ih koristim u okviru upita ali 
# sve sto sam pokusao je propalo, mozda bih uspeo u js-u ali ni to nije sigurno
def upit1_2_1():
    #query = list(db.credits_per_actor.find({},{ "_id": 0, "count": 0 }).limit(5))
    db['upit1_2_1'].drop()
    db['credits'].aggregate([
        #{ "$limit": 50},
        { "$project": { "crew": 0 } },
        { '$project': {"_id": 0} }, #potencijalno nece biti neophodan korak
        { "$unwind": "$cast"},
        { "$match": { "cast.name": { "$in": [ "Bess Flowers", "Christopher Lee", "John Wayne", "Samuel L. Jackson", "Gérard Depardieu" ] } } },
        { "$lookup":
              {
                  "from": "metadata",
                  "localField": "id",
                  "foreignField": "id",
                  "pipeline":
                    [
                        { "$project": { "id": 1, "spoken_languages": 1, "original_title": 1, "_id": 0 } },
                        { "$unwind": "$spoken_languages"}
                    ],
                  "as": "metadataForCredits"
              }
        },
        { "$unwind": "$metadataForCredits" },
        { "$group": { "_id": "$cast.name", "spoken_languages": { "$addToSet": "$metadataForCredits.spoken_languages.name" } } },
        { '$merge': 'upit1_2_1' }
    ])

#TODO Ovo je tako jadno i bedno ne mogu uopste da prebacim string u broj znaci ubicu se
def upit1_3_1():
    db['upit1_3_1'].drop()
    db['metadata'].aggregate([
        { "$match": { "vote_average": { "$lt": "4.0" } } },
        { "$addFields":
            {
                "convertedBudget": { "$toDouble": "$budget" },
                "convertedRevenue": { "$toDouble": "$revenue" },
                "convertedVoteAverage": { "$toDouble": "$vote_average" },
                "convertedVoteCount": { "$toDouble": "$vote_count" }
            }
        },
        { "$addFields":
              {
                  "profit": { "$subtract" : [ "$convertedRevenue", "$convertedBudget" ] },
                  "requiredProfit": { "$multiply": [ "$convertedBudget", 1.15 ] }
              }
        },
        { "$project":
              {
                "original_title": 1,
                "convertedBudget": 1,
                "convertedRevenue": 1,
                "profit": 1,
                "vote_average": 1,
                "vote_count": 1,
                "acceptable": { "$cond": [ { "$lt": [ "$profit",  "$requiredProfit" ] }, True, False ] }
            }
        },
        { "$match": { "acceptable": True } },
        { "$sort": { "vote_average": -1 } },
        
        { '$merge': 'upit1_3_1' }
    ])

#GOTOV:    
def upit2_1_1():   
    db['ratings_hist'].drop()
    
    t0 = time.time()
    db['ratings_small'].aggregate([
        { 
         '$group': { '_id': "$rating", 'count': { '$sum': 1 } } 
        },
        {
         '$sort' : { '_id' : 1 } 
        },
        {
         '$merge':{ 'into': "ratings_hist" }
        }
    ]);
    t1 = time.time()
    print(f'upit2_1_1, Histogram ocena, za {round(t1-t0,2)}s')

def upit2_5_1():
    
    t0 = time.time()
    db['credits_per_actor'].drop()
    db['credits'].aggregate([
        {
         '$project': {'cast': 1}
        },
        {
         '$unwind': '$cast'
        },
        {
         "$group": {
           "_id": "$cast.id",
           "name": { "$first": "$cast.name" },
           "count": { "$sum": 1 }
         }
        },
        {
         "$sort": {
          "count": -1
         }
        },
        {
         '$merge':{ 'into': "credits_per_actor" }
        }
    ])
    print(f'upit2_5_1, Broj filmova po glumcu, za {round(time.time()-t0,2)}s')
def naiveInsert():    
    insertCSV('ratings_small.csv', 'ratings_small')
    
    insertCSV('movies_metadata.csv', 'metadata', ['belongs_to_collection', 'genres', 'production_companies', 'production_countries', 'spoken_languages'])
    
    insertCSV('keywords.csv', 'keywords', ['keywords'])
    
    insertCSV('links.csv', 'links')
    
    insertCSV('credits.csv', 'credits', ['cast', 'crew'])


# Eksperiment je utvrdio da 40k x 40k lookup merge treba oko 600 sekundi
def lookupTimeExperiment(expSize = 10):
    fruitData = []
    for i in range(expSize):
        fruitData.append({
            'fruitId': i,
            'fruitName': randString(),
            'dummy1': randString(),
            'dummy2': randString(),
            'dummy3': randString(),
            'eatenBy': i
            })
        
    animalData = []
    for i in range(expSize):
        animalData.append({
            'animalId': i,
            'animalName': randString(),
            'dummy1': randString(),
            'dummy2': randString(),
            'dummy3': randString(),
            'eats': i
            })
        
    insertData(fruitData, 'fruit')
    insertData(animalData, 'animals')
    
    t0 = time.time()
    
    db.fruit.aggregate([
        {
            '$lookup': {
                'from': "animals",
                'localField': "eatenBy",
                'foreignField': "eats",
                'as': "naturalEnemy"
            }
        },
        {
         '$merge':{ 'into': "foodchain" }
        }
    ])
    
    t1 = time.time()
    
    print(f"aggregated lookup of {expSize} by {expSize} in {t1-t0}s")
    
    db['fruit'].drop()
    db['animals'].drop()
    #db['foodchain'].drop()


print("Query started...")
t0 = time.time()
upit1_3_1()
t1 = time.time()
print("Query completed in: ")
print(t1-t0)