import pymongo
import csv
from bson.code import Code
from pymongo import Connection
from hammer import *
from scipy import linspace, polyval, polyfit, sqrt, stats, randn
import itertools


(new) C:\Users\Patrick Leo\Hammer>

conn = Connection()
db = conn.hammer
db.drop_collection("data")
db.drop_collection("flags")
db.drop_collection("info")
db.drop_collection("poly")
collection = db.data
flags = db.flags
info = db.info
poly_collection = db.poly

#constants
fieldname = "year"

#read in file
file = open('sample.csv','rb')
reader = csv.DictReader(file,delimiter=',')
for item in reader :
	collection.insert(item)

keys = uniquekeys(collection)

#Regular 
print 	"Absolute:"
hammer(flags,info,collection,keys)		


db.drop_collection("flags")
db.drop_collection("info")
db.drop_collection("poly")
flags = db.flags
info = db.info
poly_collection = db.poly

#with polyfit

for doc in mr_poly(collection,fieldname,keys).find():
	new = {}
	new["value"] = polyfit(doc["value"]["x"],doc["value"]["y"],1)[0]
	
	values = doc["_id"].split("|")
	for row in keys:
		if row == fieldname:
			keys.remove(row)
			break
	for i in range(len(keys)):
		new[keys[i]] = values[i]
	poly_collection.insert(new)

print 	"Trends:"
hammer(flags,info,poly_collection,keys)		


				

		
	





				  
	


