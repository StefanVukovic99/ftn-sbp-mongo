import pymongo
from datetime import datetime
import pandas as pd
import time
import random
import string
import ast
from datetime import datetime
import pprint

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

def insertCSV(csv, name, jsonCols = [], dateCols = [], converts = {}):
    t0 = time.time()
    data = pd.read_csv(csv, dtype=str)
    data = data.astype(converts)
    data=data.to_dict('records')
        
    for document in data:
        for col in jsonCols:
            if(type(document[col]) == str): document[col] = ast.literal_eval(document[col])
        for col in dateCols:
            if (type(document[col]) == float):
                document[col] = str(document[col])
                document[col] = "1111-11-11" # resolving missing values by setting them to an impossible value
            document[col] = datetime.strptime(document[col], '%Y-%m-%d')
            
    t1 = time.time()
    print(f"read {name} in {round(t1-t0, 2)}s")
    insertData(data, name)
    
def findMinDate():
    db['min_date'].drop()
    db['metadata'].aggregate([
        { "$project": { "release_date": 1 } },
        { "$sort": { "release_date": 1 } },
        { '$merge': 'min_date' }
        ])

#DONE: Pronadji sve kljucne reci koje se pojavljuju kod filmova odredjenog zanra, i poredjaj ih od najcescih do najredjih
#TODO: Optimize... Current runtime: ~370 seconds (~6 minutes)
# =============================================================================
# def upit1_1_1(genre = 'Drama'):
#     db['upit1_1_1'].drop()
#     db['metadata'].aggregate([
#         { '$unwind': '$genres' },
#         { '$match': { 'genres.name': genre} },
#         { '$project': {'genres': 1, 'original_title': 1, 'id': 1 } },
#         { "$lookup": 
#            {
#                 "from": "keywords",
#                 "localField": "id",
#                 "foreignField": "id",
#                 "pipeline":
#                 [
#                     #{ '$project': {"_id": 0} },
#                     { '$unwind': '$keywords' }
#                 ],
#                 "as": "keywordsForGenres"
#             }
#         },
#         { '$unwind': '$keywordsForGenres' },
#         # tresem se i placem evo sat vremena smo resavali problem kog nije bilo jaoj majko
#         { "$sortByCount" : "$keywordsForGenres.keywords.name" }, #todorova magija
# 
#         { '$merge': 'upit1_1_1' }
#     ])
# =============================================================================

#DONE: Za prvih 10 glumaca iz tabele 'credits_per_actor' prikazi sve jezike koji su se govorili u svim filmovima, grupisano po glumcima
#TODO: Optimize... Current runtime: ~30 seconds (this could present a problem with an increase to the number of actors the calculation is based on)
def upit1_2_1(n = 10):
    query = list(db.credits_per_actor.find({},{ "_id": 0, "count": 0 }).limit(n))
    print('Searching for actors: ', [actor["name"] for actor in query])
    db['upit1_2_1'].drop()
    db['credits'].aggregate([
        { "$project": { "crew": 0 } },
        { '$project': {"_id": 0} }, #potencijalno nece biti neophodan korak
        { "$unwind": "$cast"},
        { "$match": { "cast.name": { "$in": [actor["name"] for actor in query] } } },
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
        
def upit1_2_2(n = 10):
    db.metadata.create_index([ ("id", 1) ])
    upit1_2_1(n)

#DONE: Pronadji sve filmove koji imaju prosecnu ocenu manju od 4, vise od 100 ocena i profit manji od 20% (filmovi sa budzetima i prihodima manjim od 10000 nisu uzeti u obzir)
#TODO: Optimize...? (query request completes in under 1 second)
def upit1_3_1():
    print("Query started...")
    t0 = time.time()
    db['upit1_3_1'].drop()
    db['metadata'].aggregate([
        { "$addFields":
            {
                "economics.convertedBudget": { "$toDouble": "$budget" },
                "economics.convertedRevenue": { "$toDouble": "$revenue" },
                "convertedVoteAverage": { "$toDouble": "$vote_average" },
                "convertedVoteCount": { "$toDouble": "$vote_count" },
            }
        },
        { "$match":
             { 
                "convertedVoteAverage": { "$lte": 4.0 },
                "convertedVoteCount": { "$gte": 100 },
                "economics.convertedRevenue": { "$gt": 10000 },
                "economics.convertedBudget": { "$gt": 10000 } 
             } 
        },
        { "$addFields":
              {
                  
                  "economics.requiredRevenue": { "$multiply": [ "$economics.convertedBudget", 1.20 ] },
                  "economics.profit": { "$subtract" : [ "$economics.convertedRevenue", "$economics.convertedBudget" ] }
              }
        },
        { "$addFields": { "economics.profitInPercent": { "$multiply": [ 100, { "$divide": [ "$economics.profit", "$economics.convertedBudget" ] } ] } } },
        { "$project":
              {
                "original_title": 1,
                "release_date": 1,
                "economics.convertedBudget": 1,
                "economics.convertedRevenue": 1,
                "economics.profit": 1,
                "economics.profitInPercent": 1,
                "economics.requiredRevenue": 1,
                "convertedVoteAverage": 1,
                "convertedVoteCount": 1,
                "acceptable": { "$cond": [ { "$lt": [ "$economics.convertedRevenue",  "$economics.requiredRevenue" ] }, True, False ] }
            }
        },
        { "$match": { "acceptable": True } },
        { "$project": { "acceptable": 0 } },
        { "$sort": { "profitInPercent": -1 } },
        { '$merge': 'upit1_3_1' }
    ])
    t1 = time.time()
    print(t1-t0)

#DONE: Prikazi ukupnu kolicinu novca ulozenu u proizvodnju filmova, kao i ukupni profit koji je ostvaren na nivou sledecih vremenskih intervala: 1980-1985, 2010-2015
#TODO: Optimize...? (query request completes in under 1 second)
def upit1_4_1():
    print("Query started...")
    t0 = time.time()
    db['upit1_4_1'].drop()
    db['metadata'].aggregate([
        { "$addFields":
            {
                "economics.convertedBudget": { "$toDouble": "$budget" },
                "economics.convertedRevenue": { "$toDouble": "$revenue" },
            }
        },
        { "$match":
             { 
                "economics.convertedRevenue": { "$gt": 10000 },
                "economics.convertedBudget": { "$gt": 10000 } 
             } 
        },
        { "$addFields":
              { "economics.profit": { "$subtract" : [ "$economics.convertedRevenue", "$economics.convertedBudget" ] } }
        },
        { "$project":
            { 
                "economics.convertedBudget": 1,
                "economics.convertedRevenue": 1,
                "economics.profit": 1,
                "convertedReleaseDate": 1,
                "release_date": 1,
                "timeGroup":
                  { "$cond":
                      [
                          { "$and":
                              [
                                  {"$gte": ["$release_date", datetime.strptime('1980-01-01', '%Y-%m-%d')]},
                                  {"$lte": ["$release_date", datetime.strptime('1985-12-31', '%Y-%m-%d')]}
                              ]
                          }, 
                          "1980-1985",
                          { "$cond":
                               [
                                   { "$and":
                                        [
                                            {"$gte": ["$release_date", datetime.strptime('2010-01-01', '%Y-%m-%d')]},
                                            {"$lte": ["$release_date", datetime.strptime('2015-12-31', '%Y-%m-%d')]}
                                        ]
                                   },
                                   "2010-2015",
                                   "---------"
                               ]
                          }
                      ]
                  }
            }
        },
        { "$match": { "$or": [ { "timeGroup": "1980-1985" }, { "timeGroup": "2010-2015", } ] } },
        { "$group":
              {
                  "_id": "$timeGroup",
                  "totalBudget": { "$sum": "$economics.convertedBudget" },
                  "totalProfit": { "$sum": "$economics.profit" },
              }
        },
        { '$merge': 'upit1_4_1' }
    ])
    t1 = time.time()
    print(t1-t0)
    
    # db['upit1_4_1'].find().sort({"economics.convertedBudget": 1}).skip(db['upit1_4_1'].count() / 2).limit(1) # find median for field, not used

#DONE: Izlistaj (ako postoje) 'Director', 'Assistant Director', 'Writer', 'Producer' i 'Executive Producer' za sve filmove u opadajucem poretku profita.
#DONE: Optimized by using an index. Time reduced from ~20 minutes (~1200 seconds) to ~12 seconds
def upit1_5_1():
    
    db.credits.create_index([ ("id", -1) ])
    
    print("Query started...")
    t0 = time.time()
    db['upit1_5_1'].drop()
    db['metadata'].aggregate([
        { "$addFields":
            {
                "economics.convertedBudget": { "$toDouble": "$budget" },
                "economics.convertedRevenue": { "$toDouble": "$revenue" }
            }
        },
        { "$addFields":
            { "economics.profit": { "$subtract": [ "$economics.convertedRevenue", "$economics.convertedBudget" ] } }
        },
        { "$lookup":
             {
                  "from": "credits",
                  "localField": "id",
                  "foreignField": "id",
                  "pipeline":
                     [
                         { "$project": { "crew": 1, "_id": 0 } },
                         { "$unwind": "$crew"},
                         { "$group":
                             {
                                 "_id": "$crew.job",
                                 "names": { "$push": "$crew.name" }
                             }
                         }
                     ],
                  "as": "metadataForCredits"
              }
        },
        { "$project": { "_id": 0 } },
        { "$unwind": "$metadataForCredits" },
        { "$project": 
              { 
                  "_id": 0,
                  "id": 1,
                  "economics.profit": 1,
                  "original_title": 1,
                  "metadataForCredits": 1
              }
        },
        { "$match": 
             { "$or":
                  [
                      { "metadataForCredits._id": { "$eq": "Director" } },
                      { "metadataForCredits._id": { "$eq": "Assistant Director" } },
                      { "metadataForCredits._id": { "$eq": "Writer" } },
                      { "metadataForCredits._id": { "$eq": "Producer" } },
                      { "metadataForCredits._id": { "$eq": "Executive Producer" } }
                  ]
             }
        },
        { "$group":
            {
                "_id": "$id",
                "title": { "$first": "$original_title" },
                "crew_member": { "$push": "$metadataForCredits" },
                "profit": { "$first": "$economics.profit" }
            }
        },
        { "$sort": { "profit": -1 } },
        { '$merge': 'upit1_5_1' }
    ], allowDiskUse = True)
    t1 = time.time()
    print(t1-t0)

#GOTOV - Histogram ocena   
def upit2_1_1():   
    db['ratings_hist'].drop()
    
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

def upit2_2_1():
    db['avg_roi_for_genre'].drop()
    db['metadata'].aggregate([
        { '$unwind': '$genres' },
        { 
         "$project" : {
          '_id' : 0,
          'id': 1,
          'genre': '$genres.name',
          'roi': { 
              "$cond": [
                  {"$or":[
                      { "$lte": [ "$revenue", 1000 ] }, 
                      { "$lte" : ["$budget", 1000] }
                  ]},
                  "N/A", 
                  {'$multiply': [
                      {'$divide': [
                          { '$subtract' : [ '$revenue', '$budget' ] }, 
                          '$budget'
                      ]},
                      100
                  ]} 
            ]},
          }
        },
        {
         '$group': {
             '_id' : '$genre',
             'expectedROI': { "$avg": "$roi" },
             }
        },
        {
         '$sort': {'expectedROI': -1}
        },
        { '$merge': {'into': 'avg_roi_for_genre' }}
    ])

#GOTOV - glumci za rezisera
def upit2_3_1(directorName = 'Quentin Tarantino'):
    db['cast_for_director'].drop()
    db['credits'].aggregate([
        { '$unwind': '$crew' },
        { '$match': { 'crew.name': directorName, 'crew.job': 'Director' } },
        { '$unwind': '$cast' },
        { '$project' : {'_id' : 0}},
        {
            "$sortByCount" : "$cast.name"
        },
        { '$merge': {'into': 'cast_for_director' }}
    ])

#GOTOV - zanrovi za glumca
def upit2_4_1(actorName = 'Eddie Murphy'):
    db['genres_for_actor'].drop()
    db['credits'].aggregate([
        { '$unwind': '$cast' },
        { '$match': { 'cast.name': actorName } },
        { '$project': { 'cast': 1, 'id': 1 } },
        { "$lookup": 
           {
                "from": "metadata",
                "localField": "id",
                "foreignField": "id",
                "pipeline":
                [
                    { '$project': {"genres": 1} },
                    { '$unwind': '$genres' }
                ],
                "as": "genresForActor"
            }
        },
        { '$unwind': '$genresForActor' },
        { '$project' : {'_id' : 0}},
        {
            "$sortByCount" : "$genresForActor.genres.name"
        },

        { '$merge': 'genres_for_actor' }
    ])

def upit2_4_2(actorName = 'Eddie Murphy'):
    db.metadata.create_index([ ("id", 1) ])
    upit2_4_1(actorName)
    
#GOTOV - broj filmova po glumcu
def upit2_5_1():
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
    
def insertCollections():    
    insertCSV('ratings_small.csv', 'ratings_small')
    
    insertCSV(
        'movies_metadata.csv', 
        'metadata', 
        jsonCols = ['belongs_to_collection', 'genres', 'production_companies', 'production_countries', 'spoken_languages'],
        converts = {
            'budget': int,
            'revenue': int
        },
        dateCols = ['release_date']
    )
    
    insertCSV('keywords.csv', 'keywords', jsonCols = ['keywords'])
    
    insertCSV('links.csv', 'links')
    
    insertCSV('credits.csv', 'credits', jsonCols =['cast', 'crew'])
    
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
    db['foodchain'].drop()

def benchmark():
    #insertCollections()
    
    times = {'basic':[], 'optimized':[]}
    for half in range (1,3):
        for n in range(1,6):
            for version in range(1,3):
                for col in db.collection_names():
                    db[col].drop_indexes()
                functionName = f"upit{half}_{n}_{version}"
                if functionName in globals():
                    t0=time.time()
                    print(f'started {functionName} at {datetime.fromtimestamp(t0).strftime("%Y-%B-%d %H:%M:%S")}')
                    globals()[functionName]()
                    timeRes = time.time() - t0
                    print(f'{functionName} za {round(timeRes,2)}s')
                    times['basic' if version == 1 else 'optimized'].append({functionName : timeRes})
    pprint.pprint(times, width=1)

benchmark()