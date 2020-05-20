import hashlib
import requests
import re
import json
from collections import defaultdict
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from flask import Flask, render_template,jsonify,request,abort,Response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import *
from datetime import datetime

import time
unique_count = 0
app=Flask(__name__)

app.config['JSON_SORT_KEYS'] = False

def make_request(url, data, headers, http_method):
	global response
	try:
		if http_method == "POST":
			response = requests.post(url, data = data, headers = headers)
			return response
		elif http_method == "GET":
			response = requests.get(url, data = data, headers = headers)
			return response

	except requests.exceptions.RequestException as e:
		return None


@app.route('/api/v1/_count',methods=["GET"])
def get_count():
	global unique_count
	res = []
	res.append(unique_count)
	return jsonify(res)


@app.route('/api/v1/_count',methods=["DELETE"])
def reset_count():
	global unique_count
	unique_count = 0
	return Response(json.dumps({}), 200)


@app.route('/api/v1/rides/count',methods=["GET"])
def count_rides():
	global unique_count
	unique_count += 1
	where_clause = "1=1"
	data = json.dumps({"table_name": "rides", "column_names": ["*"], "where": where_clause})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/read', data, headers, "POST")

	result = response.json()
	r = len(result)
	res = []
	res.append(r)
	return jsonify(res)



@app.route('/api/v1/rides',methods=["POST"])
def create_ride():
	global unique_count
	unique_count += 1
	try:
		created_by = request.get_json()["created_by"]
		timestamp = request.get_json()["timestamp"]
		source = request.get_json()["source"]
		destination = request.get_json()["destination"]
	except:
		return Response(json.dumps({"result": "Input not in correct format"}), 400)

	where_clause = "username = " + "'" + created_by + "'"
	dt0 = time.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")
	dt = time.strftime("%Y-%m-%d %H:%M:%S", dt0)

	if int(source)>198 or int(source)<1 or int(destination)>198 or int(destination)<1:
		return Response(json.dumps({"result": "Invalid source or destination"}), 400)

	if source == destination:
		return Response(json.dumps({"result": "Source and destination cannot be same"}), 400)

	data = json.dumps({})

	headers = {'Content-Type' : 'application/json'}

	response = make_request('http://CCP-816177819.us-east-1.elb.amazonaws.com/api/v1/users', data, headers, "GET")
	
	if request.method != 'POST' \
						 '':
		return Response(json.dumps({"result": "Invalid method"}), 405)
	elif created_by not in response.text:
		return Response(json.dumps({"result" : "Username not registered"}), 400)
	else:
		headers = {'Content-Type' : 'application/json'}
		data = json.dumps({"table_name" : "rides", "column_names" : ["created_by","timestamp","source","destination"] , "column_values" : [created_by, dt, source, destination], "delete_flag" : "0", "where" : "abcd"})
		#data1 = json.dumps({"table_name": "ride_users", "column_names": ["rideId", "username"], "column_values": [created_by, timestamp, source, destination]})
		write_response = make_request('http://107.22.134.120/write', data, headers, "POST")
		return Response(json.dumps({"result" : "Ride created"}), 201)


@app.route('/api/v1/rides',methods=["PUT"])
def wrong_method():
	global unique_count
	unique_count += 1
	return Response(json.dumps({"result": "Invalid method"}), 405)


@app.route('/api/v1/rides/<rideId>', methods=["DELETE"])

def delete_ride(rideId):
	global unique_count
	unique_count += 1
	where_clause = "rideId = " + "'" + rideId + "'"
	data = json.dumps({"table_name": "rides", "column_names" : ["created_by","timestamp","source","destination"] , "column_values" : ["abcd", "1234", "1", "2"], "where": where_clause, "delete_flag" : "1"})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/write', data, headers, "POST")

	
	if str(response) == "<Response [400]>":
		return Response(json.dumps({"result": "Ride does not exist"}), 400)
	elif str(response) == "<Response [200]>":
		return Response(json.dumps({"result": "Deletion successful"}), 200)


@app.route('/api/v1/db/clear', methods=["POST"])

def clear_db():

	where_clause = "1=1"
	data = json.dumps({"table_name": "rides", "column_names": ["username", "password"], "column_values": ["abc", "1234"], "where": where_clause, "delete_flag" : "1"})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/write', data, headers, "POST")

	where_clause = "1=1"
	data = json.dumps({"table_name": "ride_users", "column_names": ["username", "password"], "column_values": ["abc", "1234"], "where": where_clause, "delete_flag" : "1"})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/write', data, headers, "POST")

	
	return Response(json.dumps({"result": "Deletion successful"}), 200)




@app.route('/api/v1/rides/<rideId>', methods=["POST"])

def join_ride(rideId):
	global unique_count
	unique_count += 1

	try:
		username = request.get_json()["username"]
	except:
		return Response(json.dumps({"result": "Input not in correct format"}), 400)

	where_clause = "rideId = " + "'" + rideId + "'"
	data = json.dumps({"table_name": "rides", "column_names": ["*"], "where": where_clause})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/read', data, headers, "POST")

	where_clause = "username = " + "'" + username + "'"
	data = json.dumps({})
	response1 = make_request('http://CCP-816177819.us-east-1.elb.amazonaws.com/api/v1/users', data, headers, "GET")
	if response.json() == []:
		return Response(json.dumps({"result": "Ride does not exist"}), 400)

	elif username not in response1.json():
		return Response(json.dumps({"result": "Username does not exist"}), 400)

	else:
		data = json.dumps({"table_name": "ride_users", "column_names" : ["rideId","username"] , "column_values" : [rideId, username], "where": where_clause, "delete_flag" : "0"})
		response = make_request('http://107.22.134.120/write', data, headers, "POST")
		if str(response) == "<Response [400]>":
			return Response(json.dumps({"result": "Duplicate Entry"}), 400)
		return Response(json.dumps({"result": "Insertion successful"}), 200)



@app.route('/api/v1/rides',methods=["GET"])
def get_rides():
	global unique_count
	unique_count += 1

	try:
		source = request.args.get('source', None)
		destination = request.args.get('destination', None)
	except:
		return Response(json.dumps({"result": "Input not in correct format"}), 400)

	cur = str(datetime.now())
	where_clause = "source = " + "'" + source + "'" + " AND " + "destination = " + "'" + destination + "'" + " AND " + "timestamp >= " + "'" + cur +"'"
	data = json.dumps({"table_name": "rides", "column_names": ["rideId", "created_by", "timestamp"], "where": where_clause})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/read', data, headers, "POST")
	src = int(source)
	des = int(destination)
	if src>198 or src<1 or des>198 or des<1:
		return Response(json.dumps({"result": "Invalid source or destination"}), 400)
	elif response.json() == []:
		return Response(json.dumps({"result": "No upcoming rides for the given source and destination"}), 204)
	else:
		c = 0
		result = response.json()
		for i in result:
			dt = i["timestamp"]
			dt = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
			result[c]["timestamp"] = time.strftime("%d-%m-%Y:%S-%M-%H", dt)
			c = c+1


		return jsonify(result)





@app.route('/api/v1/rides/<rideId>',methods=["GET"])
def get_details(rideId):
	global unique_count
	unique_count += 1
	where_clause = "rideId = " + "'" + rideId + "'"
	data = json.dumps({"table_name": "rides", "column_names": ["rideId", "created_by", "timestamp", "source", "destination"], "where": where_clause})
	data1 = json.dumps({"table_name": "ride_users", "column_names": ["username"], "where": where_clause})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/read', data, headers, "POST")
	response1 = make_request('http://107.22.134.120/read', data1, headers, "POST")
	fake = []
	col = []
	if response.json() == []:
		return Response(json.dumps({"result": "Ride ID does not exist"}), 204)
	else:
		c = 0
		result = response.json()
		for i in result:
			dt = i["timestamp"]
			dt = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
			result[c]["timestamp"] = time.strftime("%d-%m-%Y:%S-%M-%H", dt)
			#print(time.strftime("%d-%m-%Y:%S-%M-%H", dt))
			c = c + 1

		for i in range(len(response1.json())):
			col.append(response1.json()[i]["username"])


		result[0]['users']=col

		return jsonify(result)


if __name__ == '__main__':
	
	app.debug=True
	app.run("0.0.0.0", debug = True)
	

