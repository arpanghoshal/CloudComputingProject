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

@app.route('/api/v1/users',methods=["PUT"])
def create_user():
	print("working")
	global unique_count
	unique_count += 1
	if request.method != 'PUT':
		return Response(json.dumps({"result": "Invalid method"}), 405)

	try:
		username = request.get_json()["username"]
		password = request.get_json()["password"]

	except:
		return Response(json.dumps({"result": "Input not in correct format"}), 400)

	if not re.match("^[a-fA-F0-9]{40}$", password):
		return Response(json.dumps({"result": "Password not in SHA1 hash hex format"}), 400)
	headers = {'Content-Type' : 'application/json'}
	data = json.dumps({"table_name" : "user", "column_names" : ["username", "password"], "column_values" : [username,password], "delete_flag" : "0", "where" : "abcd"})
	response = make_request('http://107.22.134.120/write', data, headers, "POST")
	

	if str(response) == "<Response [400]>":
		return Response(json.dumps({"result": "Duplicate entry"}), 400)
	elif str(response) == "<Response [201]>":
		return Response(json.dumps({"result": "Insertion successful"}), 201)


@app.route('/api/v1/users',methods=["POST"])
def wrong_method():
	global unique_count
	unique_count += 1
	return Response(json.dumps({"result": "Invalid method"}), 405)


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



@app.route('/api/v1/users/<username>', methods=["DELETE"])
def delete_user(username):
	global unique_count
	unique_count += 1
	where_clause = "username = " + "'" + username + "'"
	data = json.dumps({"table_name": "user", "column_names": ["username", "password"], "column_values": ["abc", "1234"], "where": where_clause, "delete_flag" : "1"})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/write', data, headers, "POST")

	where_clause = "created_by = " + "'" + username + "'"
	data = json.dumps({"table_name": "rides", "column_names": ["created_by", "timestamp", "source", "destination"], "column_values": ["abcd", "1234", "1", "2"], "where": where_clause, "delete_flag": "1"})
	response1 = make_request('http://107.22.134.120/write', data, headers, "POST")
	where_clause = "username = " + "'" + username + "'"
	data = json.dumps({"table_name": "ride_users", "column_names": ["rideId", "username"], "column_values": ["2", "abcd"], "where": where_clause, "delete_flag": "1"})
	response2 = make_request('http://107.22.134.120/write', data, headers, "POST")

	if str(response) == "<Response [400]>":
		return Response(json.dumps({"result": "Username does not exist"}), 400)
	elif str(response) == "<Response [200]>":
		return Response(json.dumps({"result": "Deletion successful"}), 200)



@app.route('/api/v1/db/clear', methods=["POST"])
def clear_db():

	where_clause = "1=1"
	data = json.dumps({"table_name": "user", "column_names": ["username", "password"], "column_values": ["abc", "1234"], "where": where_clause, "delete_flag" : "1"})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/write', data, headers, "POST")
	return Response(json.dumps({"result": "Deletion successful"}), 200)



@app.route('/api/v1/users',methods=["GET"])
def get_users():
	global unique_count
	unique_count += 1
	where_clause = "1=1"
	data = json.dumps({"table_name": "user", "column_names": ["*"], "where": where_clause})
	headers = {'Content-Type': 'application/json'}
	response = make_request('http://107.22.134.120/read', data, headers, "POST")

	if response.json() == []:
		return Response(json.dumps({"result": "No users"}), 204)
	else:
		result = response.json()
		res = []
		for item in result:
			res.append(item["username"])
		return jsonify(res)


if __name__ == '__main__':
	
	app.debug=True
	app.run(host="0.0.0.0", debug = True)
	

