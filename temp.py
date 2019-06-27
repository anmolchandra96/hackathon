# -*- coding: utf-8 -*-
"""
Spyder Editor
This is a temporary script file.
"""

"""from nltk.stem import WordNetLemmatizer 
lemmatizer = WordNetLemmatizer() 
x = lemmatizer.lemmatize('study')
y = lemmatizer.lemmatize('studying')
print x,y"""


"""from nltk.stem import PorterStemmer 
   
ps = PorterStemmer() 
  
# choose some words to be stemmed 
# words = ["program", "programs", "programer", "programing", "programers"] 
  
for w in words: 
    print(w, " : ", ps.stem(w))
   """ 
# pre processing
    
import pandas as pd
from pymongo import MongoClient
import pprint 


def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]


def read_mongo(db, collection, query ,host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    cursor = db[collection].aggregate(query)
    return list(cursor)


def find_mongo(db, collection, query ,host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    cursor = db[collection].find(query)
    return list(cursor)



def write_mongo(db, collection, query,host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    db[collection].insert(query)
    return 

def update_mongo(db, collection, query, keyword,host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    db[collection].update({'keyword':keyword},query)
    return 

if __name__ == '__main__':
    keywords = read_mongo('hackathon', 'Stories', [  { "$lookup": { "from": 'PriorityChanges', "localField": 'msid', "foreignField": 'msid', "as": 'priorities' } }, 
             { "$match": { "priorities": { "$ne": []},"keywords": { "$ne": [] },} },
             {"$project": { "keywords": 1, "msid": 1,'priorities.rank': 1,'priorities.position': 1} }])
    
    
    msid = keywords[0][u'msid']
    priorities = keywords[0][u'priorities']
    
    
    for z in keywords:
        for i in z[u'keywords']:
            key_data = find_mongo('hackathon', 'KeywordsScore', {'keyword': i})
            if len(key_data) == 0:    
                write_mongo('hackathon', 'KeywordsScore', { 'keyword': i, 'rank': priorities[0][u'rank']  })
            else:
                rank = key_data[0][u'rank'];
                update_mongo('hackathon', 'KeywordsScore', { 'keyword': i, 'rank': priorities[0][u'rank'] + rank}, i )
            
        
    
    
        
    
    
