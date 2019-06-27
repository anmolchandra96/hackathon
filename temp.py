#!/usr/bin/python2
from nltk.stem import PorterStemmer 
from nltk.corpus import stopwords

ps = PorterStemmer() 
  
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
            for w in i.split(' '):
              if w in set(stopwords.words('english')):
                break
              rank_data = find_mongo('hackathon', 'KeywordsScore', {'keyword': w})
              w = ps.stem(w)
              w.join('')
              if len(rank_data) == 0:    
                write_mongo('hackathon', 'KeywordsScore', { 'keyword': w, 'rank': priorities[0][u'rank']  })
              else:
                rank = rank_data[0][u'rank'];
                update_mongo('hackathon', 'KeywordsScore', { 'keyword': w, 'rank': priorities[0][u'rank'] + rank}, i )
            
        
    
    
        
    
    
