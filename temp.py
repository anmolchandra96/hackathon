#!/usr/bin/python2
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


def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]


def read_mongo(db, collection,  host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    query = [  { "$lookup": { "from": 'PriorityChanges', "localField": 'msid', "foreignField": 'msid', "as": 'priorities' } }, 
             { "$match": { "priorities": { "$ne": []},"keywords": { "$ne": [] },} },
             {"$project": { "keywords": 1, "msid": 1,'priorities.rank': 1,'priorities.position': 1} }]
    
        
    
    #query2 = {'msid':69919513}
    # Make a query to the specific DB and Collection
    cursor = db[collection].aggregate(query)
    
    print list(cursor)    

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))
    
   # print df
    # Delete the _id
    if no_id and '_id' in df:
        del df['_id']

    return df

if __name__ == '__main__':
    df = read_mongo('hackathon', 'Stories')
    df.to_csv('1.csv', index=False)
    


    
