#  Storage node for Assignment 6, CMPT 474, Spring 2014

# Core libraries
import os
import sys
import time
import math
import json

# Libraries that have to have been installed by pip
import redis
import requests
import mimeparse
from bottle import route, run, request, response, abort

# Local libraries
from queueservice import Queue
from vectorclock import VectorClock
# For clock merging
import itertools
from collections import defaultdict
from itertools import izip
import StringIO

DEBUG = False
base_DB_port = 3000

# These values are defaults for when you start this server from the command line
# They are overridden when you run it from test/run.py
config = { 'id': 0,
           'servers': [{ 'host': 'localhost', 'port': 6379 }],
           'hostport': base_DB_port,
           'qport': 6000,
           'ndb': 1,
           'digest-length': 1}

if (len(sys.argv) > 1):
	config = json.loads(sys.argv[1])

# Gossip globals
qport = config['qport']
queue = Queue(qport)
id = config['id']

# Connect to a single Redis instance
client = redis.StrictRedis(host=config['servers'][0]['host'], port=config['servers'][0]['port'], db=0)

# A user updating their rating of something which can be accessed as:
# curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "choices": [3, 4], "clocks": [{ "c1" : 5, "c2" : 3 }] }' http://localhost:3000/rating/bob
# Response is a JSON object specifying the new average rating for the entity:
# { rating: 5 }
@route('/rating/<entity>', method='PUT')
def put_rating(entity):
    	# Check to make sure JSON is ok
    	mimetype = mimeparse.best_match(['application/json'], request.headers.get('Accept'))
    	if not mimetype: return abort(406)

    	# Check to make sure the data we're getting is JSON
    	if request.headers.get('Content-Type') != 'application/json': return abort(415)

    	response.headers.append('Content-Type', mimetype)
    	data = json.load(request.body)
	if DEBUG:
		print "[put_rating]", data

	recieved_rating = data.get('rating')
	
    	recieved_vc = VectorClock.fromDict(data.get('clocks'))

    	# Basic sanity checks on the rating
    	if isinstance(recieved_rating, int):recieved_rating = float(recieved_rating)
    	if not isinstance(recieved_rating, float): return abort(400)

   	# Weave the new rating into the current rating list
    	key = '/rating/'+entity

    	tea_name = entity

    	# COMPUTE THE MEAN, finalrating after converge existing and recieving value
    	finalrating, choices, new_vc_list = vector_converge(tea_name,recieved_rating,recieved_vc)
	
    	# SET THE RATING, CHOICES, AND CLOCKS IN THE DATABASE FOR THIS KEY
    	if choices!=None:
    		put_to_redis(tea_name, finalrating,choices,new_vc_list) #store new score

    	# YOUR CODE HERE
    	# MERGE WITH CURRENT VALUES FOR THIS KEY
    	# REPLACE FOLLOWING WITH CORRECT FINAL RATING
    	# finalrating = 0.0
    	# SAVE NEW VALUES

    	# GOSSIP

    	# Return rating
    	return {
      		"rating": finalrating
    	}

#converge using vector clock:
#return rating(float),choices(list),vc_list(list of vc)
def vector_converge(tea_name,r_rating, r_vc):
	conv_list =[]

	if DEBUG:
		print "\n[new recieved]tea_name:" , tea_name, "rating:", r_rating,  "new v clocks:",r_vc

	rating, choices, vc_list = get_from_redis(tea_name)	
	if rating == None: # no key found for the tea
		conv_rating = r_rating
                conv_choices = [r_rating]
		conv_list.append(r_vc)
        else:
		vc = merge_dict(vc_list)
		coal = VectorClock.coalesce([vc,r_vc])
                conv = VectorClock.converge([vc,r_vc])
		if DEBUG:
			print "[previous]vc_list to vc format:", vc
			print "[previous]tea_name:" , tea_name, "rating:", rating,"choices:", choices
			print "[compare] r_vc == vc :", (r_vc == vc)
			print "[compare] r_vc > vc: " , (r_vc > vc)
			print "[compare] r_vc < vc: " , (r_vc < vc)
			print "[compare] r_vc >= vc: " ,( r_vc >= vc)
			print "[compare] coalesce = " , VectorClock.coalesce([vc,r_vc])
			print "[compare] converge = " , VectorClock.converge([vc,r_vc])
			print "[compare] r_vc in v.coalesce = ",r_vc in VectorClock.coalesce([vc,r_vc])
			print "[compare] vc in v.coalesce = ",vc in VectorClock.coalesce([vc,r_vc])
			print "[compare] r_vc in v.converge = ",r_vc in seperate_to_vc_list(conv)
			print "[compare] vc in v.converge = ",vc in seperate_to_vc_list(conv)
			print "[compare] r_vc < v.converge ", r_vc <=  VectorClock.converge([vc,r_vc])
		   	print "[compare] vc < v.converge ", vc <=  VectorClock.converge([vc,r_vc])
		
				
		is_incomparable = (r_vc in coal) and (vc in coal) and ( r_vc <= conv ) and ( vc <= conv)

		if r_vc == vc:
			conv_rating = r_rating
                	conv_choices = None
			conv_list = None
		elif r_vc > vc: # more recent data
			#compute mean value,recent vector clocksd, and choices
			conv_rating = r_rating 
			conv_choices = [r_rating] 
			conv_list.append(r_vc)
		elif r_vc < vc: # ignore 
			if DEBUG:
				print "[ignore] r_vc<vc"
                        conv_rating = rating
                        conv_choices = None
			conv_list = None	
		elif is_incomparable:
			combined_clocks_list = VectorClock.coalesce([vc,r_vc])
			if DEBUG:
				print "combined clocks:",combined_clocks_list
				print "----- [incom] r_vc:", r_vc
				print "----- [incom] vc:", vc
				print "----- [incom] coal:", coal
				print "----- [incom] conv:", conv		
				print "----- [incom] choices:",choices, ",r_choices:", r_rating
		 	conv_vc_list = seperate_to_vc_list(conv)	
			
			#find choices related with conv_list
			conv_choices = []
			conv_list=[]
			conv_list,conv_choices = decide_to_append_vc_list(vc_list,choices,conv_vc_list,conv_list,conv_choices)  #check previous vc list
							
			conv_list,conv_choices = decide_to_append_vc_list([r_vc],[r_rating],conv_vc_list,conv_list,conv_choices)#check received r_vc list  
		
			conv_list,conv_choices = eliminate_old_clocks(conv_list,conv_choices) #double check
			
			conv_rating = meanAvg(conv_choices)

			if DEBUG:	
				print "----- [incomp] new conv_choices:",conv_choices
				print "----- [incomp] new conv_list:", conv_list
				print "----- [incomp] new conv_rating:",conv_rating	
									
			
	
	return conv_rating, conv_choices,conv_list

#put format
#rating = float ex 2.3
#choices =  dictionay ex) [1,2]
#clocks = vc.clock  ex) {'c1': 10, 'c0': 7} --> dictionary
#clocks list  ex) [{"c4": 100}, {"c23": 13, "c5": 21}]
def put_to_redis(tea_name, rating, dic_choices, vc_list):
	
	json_data = result({ 'rating':rating,'choices':dic_choices, 'clocks': convert2json(vc_list) })

	if DEBUG:
		print "[put to redis]json_data:", json_data	
        key = '/rating/'+tea_name	
	client.set(key, json_data) #insert json data

def eliminate_old_clocks(conv_list,conv_choices):
	if DEBUG:
		print "[eleminate] before conv_list =", conv_list
	vc_list = []
	vc_choices =[]
	vc_list_index=0
	for vc in conv_list:
		if vc not in vc_list:
			if not has_bigger_in_vc_list(vc,conv_list):
				vc_list.append(vc)
				vc_choices.append(conv_choices[vc_list_index])
			else:	
				if DEBUG:
					print "[eleminate] found bigger vc=", vc

		vc_list_index+=1
	if DEBUG:
		print "[eleminate] after conv_list =", vc_list
	return vc_list,vc_choices
		
def decide_to_append_vc_list(vc_list,choices,conv_vc_list,conv_list,conv_choices):
	
	countOfChoices=len(choices)
        vc_list_index =0
        vc_inner_exist = False
        for vc_inner in vc_list:
        	vc_inner_exist = exist_vc_in_vc_list(vc_inner,conv_vc_list)
              	if vc_inner_exist:
			if countOfChoices==1:
				conv_choices.append(choices[0])
			else:
				conv_choices.append(choices[vc_list_index])
				
			conv_list.append(vc_inner)
              	vc_list_index+=1
	return conv_list,conv_choices

def has_bigger_in_vc_list(vc,vc_list):
	found=False
	for c in vc_list:
		if c > vc:
			found=True
	return found
		
def exist_vc_in_vc_list(vc,vc_list):
        exist=False
	vc_list_merged = merge_dict(vc_list)
	
	for c in seperate_to_vc_list(vc):
        	if c in vc_list:
			exist = True
	if DEBUG:
        	print "[check exist vc]  vc:", vc, "  exist:", exist

	if(exist):
		return True
	else:
		return False

def exist_vc_allkey_in_vc_list(vc,vc_list):
        existAll=True
	conv_dict = convert_vc_list2dict(vc_list)
  		
	if DEBUG:
		print "*** conv_vc dic keys:",conv_dict.keys()
        for c in seperate_to_vc_list(vc):
		key = c.clock.keys()[0]
		value = c.clock.values()[0]
		if DEBUG:
			print "** [check]key:" ,key,value
		
		if key not in conv_dict.keys():
                	existAll = False
			if DEBUG:
				print "** [not exist]:" ,key

        return existAll
 
#convert vector clock list to json clock list
def convert2json(vc_list):
  	return "[%s]" % ", ".join(["%s" % vc.asJsonDict() 
                                   for vc in vc_list  ])	

#convert json clock list to vector clock list        
def convert2vc_list(json_clocks_list):
	return [VectorClock.fromDict(clocks) for clocks in eval(json_clocks_list)]		

def convert_vc_list2dict(vc_list):
	json_dict = "{%s}" % ", ".join(["%s" % vc.asItemString()
                                   for vc in vc_list ])
	return eval(json_dict)
 
#merge dictionary in the list as one clocks
def merge_dict(vc_list):
        json_dict = "{%s}" % ", ".join(["%s" % vc.asItemString()
                                   for vc in vc_list ])
	return VectorClock.fromDict(eval(json_dict))

def seperate_to_vc_list(vc):
        #json_list = "[%s]" % ", ".join(["{'%s':%d}" % (k,)
        #                           for k  in vc.asSortedDict() ])
        json_list = vc.asSortedListSeperate()
	#print "vc to list(seperate):",json_list

        return convert2vc_list(json_list)

# return format
# -- rating(float)     ex) 5.0
# -- choices(dictionar) ex) [1,3]
# -- vc(VectorClock)
# if there is no key for the tea,
# -- reurn None,None,None
# Original JSON Format:  {"rating": 1.0, "clocks": {"c0": 1}, "choices": [10, 20]}
def get_from_redis(tea_name):
	
        try:
		key = '/rating/' + tea_name
                data = eval(client.get(key))
        	if DEBUG:
			print "** [get_from_redis]:data=",data
        except:
                #Error [get from redis] no key found for the tea:" + tea_name
                return None,None,None

        try:
                rating = data["rating"]
        except:
                rating = data["rating"]

        choices = data["choices"]
         
        json_clocks_list = data["clocks"]
	
	vc_list = convert2vc_list(json_clocks_list)
 	
	return rating, choices, vc_list

def meanAvg(choices):
        sumRating =0
        counter=0
        for i in choices:
                sumRating +=i
                counter +=1
        if counter !=0:
                return sumRating/counter
        else:
                return None

#json format
# -- choices: [10,20]
# -- clocks: [ { "c0": 5 }, { "c1": 3 } ]
def result(r):
       #print "jsondumps:" , json.dumps(r)
       return json.dumps(r)

# Get the aggregate rating of entity
# This can be accesed as:
#   curl -XGET http://localhost:3000/rating/bob
# Response is a JSON object specifying the mean rating, choice list, and
# clock list for entity:
#   { rating: 5, choices: [5], clocks: [{c1: 3, c4: 10}] }
# This function also causes a gossip merge
@route('/rating/<entity>', method='GET')
def get_rating(entity):
     	#key = '/rating/' + entity
	rating, choices, vc_list = get_from_redis(entity)
	print "[get_rating] ", entity, rating,choices,vc_list

	if rating == None:
		rating_format = "0"
	else:
		rating_format = format(rating, '.2f').rstrip('0').rstrip('.') 

        if choices == None:
		choices_format = "[]"
	else:
		choices_format = result(choices)

	if vc_list == None:
		vc_list_format = "[]"
	else:
		vc_list_format = convert2json(vc_list)

	 	
	# YOUR CODE HERE
	# GOSSIP
    	# GET THE VALUE FROM THE DATABASE
    	# RETURN IT, REPLACING FOLLOWING
	
	if DEBUG:
		print "[get rating]rating_f:" , rating_format
		print "[get_rating]json vc_list:",vc_list_format
	
	return { "rating":rating_format , 
		 "choices": choices_format, 
		 "clocks":vc_list_format
	}


# Delete the rating information for entity
# This can be accessed as:
#   curl -XDELETE http://localhost:3000/rating/bob
# Response is a JSON object showing the new rating for the entity (always null)
#   { rating: null }
@route('/rating/<entity>', method='DELETE')
def delete_rating(entity):
    	# ALREADY DONE--YOU DON'T NEED TO ADD ANYTHING
    	count = client.delete('/rating/'+entity)
    	if count == 0: return abort(404)
    	return { "rating": None }

# Fire the engines
if __name__ == '__main__':
	run(host='0.0.0.0', port=os.getenv('PORT', config['hostport']), quiet=True)
