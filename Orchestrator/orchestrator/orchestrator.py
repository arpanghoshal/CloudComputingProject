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
import pika
import os
import uuid
import docker
import threading
import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState

logging.basicConfig()

zk = KazooClient(hosts='zoo:2181')

def zk_listener(state):
	if(state == KazooState.LOST):
		logging.warning("Zookeeper connection lost")
	elif(state == KazooState.SUSPENDED):
		logging.warning("Zookeeper connection suspended")
	else:
		logging.info("Zookeeper connected")

zk.add_listener(zk_listener)
zk.start()

if(zk.exists("/Workers")):
	zk.delete("/Workers", recursive=True)

@zk.ChildrenWatch("/Workers/",send_event = True)
def watch_children(children,event):
	print("Children are now: %s" % children)
	if(event == None):
		pass
	elif(event.type is DELETED):
		print("Slave deleted")

timer_start_flag = False
timer_started_flag = False
read_request_count = 0
containers_running = {}
pid_name_mapping = {}
 
containers_running_index = 0

client = docker.from_env()
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
x_client = docker.APIClient(base_url='unix://var/run/docker.sock')

containers_running[containers_running_index] = client.containers.run(image="slave:latest", command =["python","slave.py"], \
detach=True,network = 'rss_default',volumes = {'/home/ubuntu/RSS/codedata':{'bind': '/code/sdb'}})
containers_running_index +=1
time.sleep(5)

client.containers.run(image="master:latest", command =["python","master.py"], \
detach=True,network = 'rss_default')
time.sleep(5)

client.containers.run(image="shared_db:latest", command =["python","shared_db.py"], \
detach=True,network = 'rss_default',volumes = {'/home/ubuntu/RSS/codedata':{'bind': '/code/sdb'}})
time.sleep(5)


def trigger_timer():
	
	global timer_started_flag
	global timer_start_flag
	
	if(not (timer_started_flag) and (timer_start_flag)):
		timer_started_flag = True
		scale_timer()

def scale_timer():
	
	print('timer')
	global read_request_count
	global containers_running
	global pid_name_mapping
	global containers_running_index
	global client
	global x_client

	if(read_request_count == 0):
		no_of_slaves = 1
	elif(read_request_count%20 == 0):
		no_of_slaves = int(read_request_count/20)
	else:
		no_of_slaves = int(read_request_count/20) + 1
	if(len(containers_running.keys()) <= no_of_slaves):
		t = len(containers_running.keys())
		for i in range(t,no_of_slaves):
			
			containers_running[containers_running_index] = client.containers.run(image="slave:latest", command =["python","slave.py"], \
			detach=True,network = 'rss_default',volumes = {'/home/ubuntu/RSS/codedata':{'bind': '/code/sdb'}})
			time.sleep(5)
			
			Name = containers_running[containers_running_index].name
			
			Pid = x_client.inspect_container(Name)['State']['Pid']
			pid_name_mapping[containers_running_index] = {'Name':Name, 'Pid': Pid}
			containers_running_index +=1
	
	elif(len(containers_running.keys()) > no_of_slaves):
		while len(containers_running.keys()) > no_of_slaves:
			containers_list = list(containers_running.keys())
			if(len(containers_list) == 0):
				min_pid = -1
			else:
				min_pid = containers_list[0]
			for i in range(0,containers_running_index):
				if(i in containers_running.keys() and x_client.inspect_container(containers_running[i].name)['State']['Pid'] < x_client.inspect_container(containers_running[min_pid].name)\
				['State']['Pid'] ):
					min_pid = i
			
			if(min_pid is not -1):
				containers_running[min_pid].stop()
				containers_running[min_pid].remove()
				del containers_running[min_pid]
				del pid_name_mapping[min_pid]
	
	print(containers_running)
	
		
	read_request_count = 0
	timer = threading.Timer(120, scale_timer)
	timer.start()


class RPCClient(object):

	rpcServer = 'readQ'
	data = json.dumps({})
	corr_id = None
	
	def __init__(self,body,rpcServer):
		
		self.rpcServer = rpcServer
		self.data = str(body)
		
		self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
		
		self.channel = self.connection.channel()
		result = self.channel.queue_declare(queue='responseQ')
		
		self.channel.basic_qos(prefetch_count=0)

		self.channel.basic_consume(on_message_callback=self.callbackResponse, queue='responseQ')
		
	def callbackResponse(self,ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body
				
	def call(self):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange="", routing_key=self.rpcServer, properties = pika.BasicProperties(reply_to = 'responseQ',correlation_id = self.corr_id),body = self.data)
		
		while self.response is None:
			self.connection.process_data_events()
		
		self.connection.close()

		return self.response

app=Flask(__name__)

@app.route('/')
def hello():
	return 'Hello World! I have been seen'

@app.route("/read" , methods=['POST'])
def readFromDB():

	global timer_start_flag 
	global read_request_count 
	timer_start_flag = True
	read_request_count +=1
	trigger_timer()

	table_name = request.get_json()["table_name"]
	column_names = request.get_json()["column_names"]
	where_clause = request.get_json()["where"]
	comma_sep_column_names = ",".join(column_names)
	statement = "SELECT " + comma_sep_column_names + " FROM " + table_name + " WHERE " + where_clause + ";"
	newClient = RPCClient(statement,'readQ')
	res = newClient.call()
	data = json.loads(res)
	code = data["code"]
	op = data["msg"]
	print(res)
	if(code == 400):
		return Response(json.dumps({"result": "Duplicate entry"}), 400)
	elif(code == 200):
		return jsonify(op)
	elif(code == 204):
		return Response(json.dumps({"result": "No data"}), 204)

@app.route("/write" , methods=['POST'])
def writeToDB():
	
	try:
		table_name = request.get_json()["table_name"]
		column_names = request.get_json()["column_names"]
		column_values = request.get_json()["column_values"]
		delete_flag = request.get_json()["delete_flag"]
		where_clause = request.get_json()["where"]
	except:
		table_name = request.get_json()["table_name"]
		column_names = request.get_json()["column_names"]
		column_values = request.get_json()["column_values"]
		delete_flag = 0

	comma_sep_column_names = ",".join(column_names)
	comma_sep_column_values = ",".join("'{0}'".format(x) for x in column_values)

	if delete_flag == '1':
		statement = "DELETE" +  " FROM " + table_name + " WHERE " + where_clause + ";"
	else:
		statement = "INSERT INTO " + table_name + " (" + comma_sep_column_names + ") " + "VALUES (" + comma_sep_column_values + ");"

	newClient = RPCClient(statement,'writeQ')
	res = newClient.call()
	data = json.loads(res)
	code = data["code"]
	print(res)
	if(code == 400):
		return Response(json.dumps({"result": "Duplicate entry"}), 400)
	elif(code == 201):
		return Response(json.dumps({"result": "Insertion Successful"}), 201)
	elif(code == 200):
		return Response(json.dumps({"result": "Successful"}), 200)
	elif(code == 204):
		return Response(json.dumps({"result": "No data"}), 204)
	
@app.route("/api/v1/crash/slave" , methods=['POST'])
def crashSlave():
	global containers_running_index
	global client
	global x_client
	global pid_name_mapping
	containers_list = list(containers_running.keys())
	if(len(containers_list) == 0):
		max_pid = -1
	else:
		max_pid = containers_list[0]
	for i in range(0,containers_running_index):
		if(i in containers_running.keys() and x_client.inspect_container(containers_running[i].name)['State']['Pid'] > x_client.inspect_container(containers_running[max_pid].name)\
		['State']['Pid'] ):
			max_pid = i

	if(max_pid is not -1):
		containers_running[max_pid].stop()
		containers_running[max_pid].remove()				
		del containers_running[max_pid]
		del pid_name_mapping[max_pid]
		containers_running[containers_running_index] = client.containers.run(image="slave:latest", command =["python","slave.py"], \
		detach=True,network = 'rss_default',volumes = {'/home/ubuntu/RSS/codedata':{'bind': '/code/sdb'}})
		Name = containers_running[containers_running_index].name	
		Pid = x_client.inspect_container(Name)['State']['Pid']
		pid_name_mapping[containers_running_index] = {'Name':Name, 'Pid': Pid}
		containers_running_index +=1
		time.sleep(5)
		return Response(json.dumps({"result": "Slave successfully crashed"}), 200)
	else:
		return Response(json.dumps({"result": "No slave to crash"}), 400)

@app.route("/api/v1/worker/list" , methods=['GET'])
def workersList():
	global containers_running_index
	global client
	global x_client
	containers_list = list(containers_running.keys())
	if(len(containers_list) == 0):
		res = []
		return jsonify(res)
	else:
		res = []
		for i in range(0,containers_running_index):
			if(i in containers_running.keys()):
				res.append(int(x_client.inspect_container(containers_running[i].name)['State']['Pid']))
		res.sort()
		return jsonify(res)

if __name__ == '__main__':
	
	app.debug=True
	app.run(host="0.0.0.0", debug = True)
