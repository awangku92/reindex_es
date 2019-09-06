import pandas as pd, numpy as np
import os, json

#index to elastic
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.helpers import bulk

# SETTING - change accordingly
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es_src = es
es_des = es

'''!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
**WARNING** FOR MULTIPLE SAME INDEX NAME EG.'yellow-*' 
BE VERY CAREFUL WITH INDEX NAME, MAKE SURE TO SNAPSHOT BEFORE REINDEX
WRONG INDEX NAME WILL RESULT IN ALL DOCUMENT FROM MULTIPLE INDEX 
TO BE REINDEX IN ONE INDEX
**IGNORE THIS WARNING IF YOU WANT TO REINDEX TO ONE INDEX
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'''
src_index_name = 'yellow*'				# SHOULD BE SPECIFIC, EG. yellow-twitter NOT yellow* 
des_index_name = 'reindex_yellow_new_field' 	# SHOULD BE SPECIFIC, EG. reindex-yellow-twitter
alias_name = 'yellow_reindex' 					# MUST MAKE SURE SEARCHABLE IN INDEX PATTERN 
'''!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
**WARNING** FOR MULTIPLE SAME INDEX NAME EG.'yellow-*' 
BE VERY CAREFUL WITH INDEX NAME, MAKE SURE TO SNAPSHOT BEFORE REINDEX
WRONG INDEX NAME WILL RESULT IN ALL DOCUMENT FROM MULTIPLE INDEX 
TO BE REINDEX IN ONE INDEX
**IGNORE THIS WARNING IF YOU WANT TO REINDEX TO ONE INDEX
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'''

# DONT CHANGE THIS!
new_field = False

def createAlias():
	# use es.indices instead of instantiating IndicesClient
	es.indices.put_alias(index=src_index_name, name=alias_name)

	print('Success creating alias')


def createNewIndexMapping():
	mappings = mappingBody()
	index = des_index_name
	try:
		if not es.indices.exists(index):
			print("Creating Index")
			e = es.indices.create(index= index, ignore=400, body=mappings, request_timeout=40)
			print("Successfully Created "+str(e))
		else:
			print("Index Exists")
	except Exception as excpt:
		print("Index Already Exists: " + str(excpt))

	print("Done creating new index mapping "+ des_index_name)


def createNewField(): 
	# Use the scan&scroll method to fetch all documents from your old index
	res = helpers.scan(es, query={
	  "query": {
		"match_all": {}

	  },
	  # "size":1000 # must know doc size 
	},index=src_index_name)

	new_insert_data = []

	# get mappping body, make sure mapping body has new field and/or new datatype
	mapping = mappingBody() 

	# i = 1 # to count doc no

	# Change the mapping and everything else by looping through all your documents
	for x in res:
		# print(i) # to count doc no

		x['_index'] = des_index_name
		# add new field, copy "content" to "tokenizedContent"
		x['_source']['tokenizedContent'] = x['_source']['Content']

		# del x['_source']['address'] # delete field
		# del x['_score']

		# refresh indices
		# es.indices.refresh(index=des_index_name)

		# Add the new data into a list
		new_insert_data.append(x)

		# i += 1 # to count doc no

	# es.indices.refresh(index=des_index_name)
	# print(new_insert_data)

	#Use the Bulk API to insert the list of your modified documents into the database
	helpers.bulk(es,new_insert_data, index=des_index_name, raise_on_error=True, request_timeout=1024)
	print("Done creating new field "+ des_index_name)

	return True # True for new_field


def reindex():
	body = {"query": {"match_all": {} }} # get all document in index
	helpers.reindex(es_src, src_index_name, des_index_name, target_client=es_des, query=body)

	print('Success reindexing')


def updateAlias():
	es.indices.update_aliases({
		"actions": [
			{ "add":    { "index": des_index_name, "alias": alias_name }}, 
			{ "remove_index": { "index": src_index_name }} # just like DELETE INDEX  
		]
	})

	es.indices.refresh(index=des_index_name)

	print('Success update alias, delete old index and refresh new index')


def mappingBody():
	return {
		"settings": {
				"analysis": {
					"analyzer": {
						"my_analyzer": {
							"type": "standard",
							# "type": "stop",
							# "stopwords": "_english_, https",
							"stopwords_path": "stopwords/custom_stopwords.txt"
						}
					}
				}
			},
		"mappings": {
			# "mentionlytics_test_atm":{
			"properties": {
				'campaign': {
					"type": "keyword"
				},
				'commtrack': {
					"type": "keyword"
				},
				'Id': {
					"type": "keyword"
				},
				'Datetime': {
					"type": "date"
				},
				# 'Date':{ "type":"date", "format": "dd MMM yyyy" },
				# 'Time':{ "type":"date", "format": "HH:mm" },
				'Channel': {
					"type": "keyword"
				},
				'Type': {
					"type": "keyword"
				},
				'Link': {
					"type": "keyword"
				},
				'Title': {
					"type": "keyword"
				},
				'Language': {
					"type": "keyword"
				},
				'Content': {
					"type": "text",
					# "fielddata": True,
					# "analyzer": "my_analyzer",
					# # "filter": "my_stop",
					# "fields": {
					# 	"raw": {
					# 		"type": "keyword"
					# 	}
					# }
				},
				'tokenizedContent': {
					"type": "text",
					"fielddata": True,
					"analyzer": "my_analyzer",
					# "filter": "my_stop",
					"fields": {
						"raw": {
							"type": "keyword"
						}
					}
				},
				'Profile': {
					"type": "keyword"
				},
				'Profile Id': {
					"type": "keyword"
				},
				'Profile Username': {
					"type": "keyword"
				},
				'Profile Description': {
					"type": "keyword"
				},
				'followers_count': {
					"type": "integer"
				},
				'Country': {
					"type": "keyword"
				},
				'profile_meta': {
					"type": "keyword"
				},
				'Sentiment': {
					"type": "keyword"
				},
				'profile_image': {
					"type": "keyword"
				},
				'comments': {
					"type": "integer"
				},
				'favorite_count': {
					"type": "integer"
				},
				'fb_shares': {
					"type": "integer"
				},
				'likes': {
					"type": "integer"
				},
				'linked_shares': {
					"type": "integer"
				},
				'post_count': {
					"type": "integer"
				},
				'retweets': {
					"type": "integer"
				},
				'share_count': {
					"type": "integer"
				},
				'statuses_count': {
					"type": "integer"
				},
				'total_count': {
					"type": "integer"
				},
				'tw_shares': {
					"type": "integer"
				},
				'views': {
					"type": "integer"
				}
			}
			# }
		}
	}


if __name__ == '__main__':
	print('Start')

	# REINDEX DATA WITH NO DOWNITME
	# 1) POINT EXISTING INDEX TO ALIAS
	createAlias()

	# 2a) CREATE NEW MAPPING
	createNewIndexMapping()

	# 2b)CREATE NEW FIELD (OPTIONAL) COMMENT IF NOT NEEDED
	# MAKE SURE mappingBody() HAS NEW FIELD IF WANT TO USE THIS
	new_field = createNewField()

	# 3) REINDEX - if create new_field = True, cannot use reindex
	if (new_field == False):
		reindex()

	# 4) POINT NEW INDEX TO ALIAS AND DELETE OLD INDEX
	updateAlias()

	print('Finish reindexing..')