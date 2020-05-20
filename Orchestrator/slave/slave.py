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
import shutil
import socket
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

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

zk.ensure_path("/Workers/")

cid = socket.gethostname()
print(cid)
path = "/Workers/"+cid

if zk.exists(path):
    print("Node already exists")
else:
    zk.create(path, b"slave node")

dest = shutil.copyfile('/code/sdb/mydatabase.db', '/code/mydatabase.db')
app=Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db_uri = 'sqlite:////code/sdb/mydatabase.db'
app.config['SQLALCHEMY_DATABASE_URI'] = db_uri

db = SQLAlchemy(app)

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('rabbitmq')
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='readQ')

class user(db.Model):
	__tablename__ = 'user'
	username = db.Column(db.String(50), primary_key=True)
	password = db.Column(db.String(50))

class ride(db.Model):
	__tablename__ = 'rides'
	rideId = db.Column(db.Integer, primary_key=True)
	created_by = db.Column(db.String(50))
	timestamp = db.Column(db.DateTime)
	source = db.Column(db.Integer)
	destination = db.Column(db.Integer)

class ride_users(db.Model):
	__tablename__ = 'ride_users'
	rideId = db.Column(db.Integer, primary_key=True)
	username = db.Column(db.String(50), primary_key=True)

db.create_all()


def responseQueueFill(body,ch,properties,method):
	json_body = json.dumps(body)
	ch.basic_ack(delivery_tag=method.delivery_tag)
	ch.basic_publish(exchange="", routing_key='responseQ',properties=pika.BasicProperties(correlation_id = properties.correlation_id),body=json_body)


def callback1(ch, method, properties, body):
	print("callback1 function working")
	
	statement = str(body)
	statement = statement.strip("b")
	statement = statement.strip("\'")
	statement = statement.strip("\"")
	statement = text(statement)
	try:
		result = db.engine.execute(statement.execution_options(autocommit = True))
		result = result.fetchall()
		op = []
		for i in result:
			op.append(dict(i))
		res = {"code": 200}
		res["msg"] = op
		responseQueueFill(res,ch,properties,method)
	except:
		res = {"code": 204, "msg": "Does not exist"}
		responseQueueFill(res,ch,properties,method)
	
	print(" [x] Received CallBack1 %r \n" % body)

def callback2(ch, method, properties, body):

	statement = str(body).strip("b")
	statement = statement.strip("\'")
	statement = statement.strip("\"")
	if "DELETE" in statement:
		statement = text(statement)
		result = db.engine.execute(statement.execution_options(autocommit=True))
		if result.rowcount == 0:
			res = {"code": 400, "msg": "Bad request"}
			responseQueueFill(res,ch,properties)
		else:
			res = {"code":200, "msg": "Deletion Successful"}
			responseQueueFill(res,ch,properties)

	else:
		statement = text(statement)
		
		try:
			result = db.engine.execute(statement.execution_options(autocommit=True))
			res = {"code": 200, "msg": "Insertion Successful"}
			responseQueueFill(res,ch,properties)
		except IntegrityError:
			res = {"code": 400, "msg": "Duplicate entry"}
			responseQueueFill(res,ch,properties)
	
	print(" [x] Received %r \n" % body)

channel.basic_consume(on_message_callback = callback1, queue = 'readQ') #,  no_ack=True
print(' [*] Waiting for -----READ---- messages. To exit press CTRL+C')
channel.start_consuming()

if __name__ == '__main__':
	
	app.debug=True
	app.run(host="0.0.0.0", debug = True)