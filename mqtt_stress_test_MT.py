#!/usr/bin/env python3

import random
import os
import binascii
import queue as Queue
import threading
import time
import urllib.parse as urlparse
import configparser as ConfigParser
import paho.mqtt.client as mqtt
import random
import datetime
import threading


pub_messages = 0
sub_messages = 0

myThr = []

filename = "report_"+datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
	
default_cfg = {
	'url'        : '52.192.103.34:1883',
	'clientid'   : 'stress',
	'msg_num'    : '100',
	'sleep_min'  : '0.01',
	'sleep_max'  : '0.50',
	'threads'    : '50',
	'topic'      : '/hw/stress',
	'msg'        : 'abcd'
	}
		
cp = ConfigParser.SafeConfigParser(default_cfg)
cp.read(os.path.splitext(__file__)[0] + '.ini')

print('Config read from: ' + os.path.splitext(__file__)[0] + '.ini')


print('\n==================================')
print('url       : ' + cp.get('MQTT','url'))
print('clientid  : ' + cp.get('MQTT','clientid'))
print('msg_num   : ' + str(cp.getint('MESSAGES','msg_num')))
print('sleep_min : ' + str(cp.getfloat('MESSAGES','sleep_min')))
print('sleep_max : ' + str(cp.getfloat('MESSAGES','sleep_max')))
print('threads   : ' + str(cp.getint('MESSAGES','threads')))
print('pub threads : ' + str(cp.getint('MESSAGES','num_pub_thread')))
print('topic     : ' + cp.get('MESSAGES','topic'))
print('msg       : ' + cp.get('MESSAGES','msg'))
print('==================================\n')

the_topic_root = cp.get('MESSAGES','topic')
num_pub_thread = cp.getint('MESSAGES','num_pub_thread')

# Queue: workers threads, and mqtt loop
queue  = Queue.Queue(maxsize = cp.getint('MESSAGES','threads')*2)
queuep = Queue.Queue(maxsize = 3)

class ThreadPub(threading.Thread):
	"""Threaded Publish"""
	def __init__(self, queue, client, min_time, max_time, msg_root, topic_root):
		threading.Thread.__init__(self)
		self.queue = queue
		self.client = client
		self.min_time = min_time
		self.max_time = max_time
		self.msg_root = msg_root
		self.topic_root = topic_root

	def run(self):
		while True:
			#grabs parameters from queue
			item = self.queue.get().strip().decode('utf-8')

			#waits and publishes
			time.sleep(random.uniform(self.min_time, self.max_time))
			msg = self.msg_root
			topic = self.topic_root + "/" + item
			self.client.publish(topic, msg)

			#signals to queue job is done
			self.queue.task_done()


class ThreadMqtt(threading.Thread):
	"""Threaded MQTT Protocol"""
	def __init__(self, queue, client):
		threading.Thread.__init__(self)
		self.queue = queue
		self.client = client

	def run(self):
		while True:
			#grabs parameters from queue
			item = self.queue.get()
			print("item:%s" %item)

			#mqtt loop until "stop"
			while item == "start":
				try:
					item = self.queue.get_nowait()
				except Queue.Empty:
					self.client.loop()
			#signals to queue job is done
			self.queue.task_done()


def on_connect(client, userdata, rc, t):
	"""
	Define event callbacks
	"""
	print("Connected with rc: " + str(rc))
	print("Subscribe Topic: " +str(the_topic_root))
	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	# QoS = 0
	if (the_topic_root.find("#")<0):
		client.subscribe(the_topic_root + "/#", 0)
	else:
		client.subscribe(the_topic_root, 0)


def on_message(client, userdata, msg):
	global sub_messages
	sub_messages += 1
	#print_log("Received on: " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def on_publish(client, userdata, mid):
	global pub_messages
	pub_messages += 1
	#print_log("Published: " + str(mid))


def on_subscribe(client, userdata, mid, granted_qos):
	print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(client, userdata, level, string):
	print(string)


def print_log(string):
	# Uncomment to enable time consuming print debug messages
	print(string)
	pass

def MQTT_Client():
	print("Run MQTT_Client")
	tid = threading.get_ident()
	print_log("Run on %s" %tid)
	global filename
	global pub_messages
	global sub_messages
	id = str(cp.get('MQTT','clientid'))
	serie = binascii.b2a_hex(os.urandom(2)).strip().decode('utf-8')
	mqttc = mqtt.Client(id+serie, clean_session=True)
	# Assign event callbacks
	mqttc.on_message    = on_message
	mqttc.on_connect    = on_connect
	mqttc.on_publish    = on_publish
	mqttc.on_subscribe  = on_subscribe
	# Uncomment to enable paho debug messages
	# mqttc.on_log        = on_log

	# Parse MQTT_URL (or fallback to localhost)
	url_str = cp.get('MQTT','url')
	if url_str.find('mqtt://') < 0:
		url_str = 'mqtt://' + url_str
	print("Trying url: " + url_str)
	url = urlparse.urlparse(url_str)

	# Connect
	print("Disconnect just in case")
	mqttc.disconnect()
	print("Now connecting...")
	mqttc.connect(url.hostname, url.port, 60)

	queuep.put("start")
	p = ThreadMqtt(queuep, mqttc)
	p.setDaemon(True)
	p.start()

	# Spawn a pool of threads, and pass them queue instance
	for i in range(cp.getint('MESSAGES','threads')):
		t = ThreadPub(
			queue, mqttc,
			cp.getfloat('MESSAGES', 'sleep_min'),
			cp.getfloat('MESSAGES', 'sleep_max'),
			cp.get('MESSAGES', 'msg'),
			cp.get('MESSAGES', 'topic')
		)
		t.setDaemon(True)
		t.start()


	for i in range(0,2000):
			
		start = time.time()
		gen_messages = 0
		try:
			while gen_messages < cp.getint('MESSAGES','msg_num') * (i+1):
				#u = binascii.b2a_hex(os.urandom(4))
				u = binascii.b2a_hex(str(tid).encode())
				gen_messages += 1
				queue.put(u)

		except KeyboardInterrupt:
			print("Keyboard interrupt");

		# print(
			# "Messages Generated: " + str(gen_messages) + "  Published: " + str(pub_messages)
			# + "  Received: " + str(sub_messages) + "  so far, waiting publishing...")
		# Wait on the queue until everything has been processed
		queue.join()

		# Wait 5 more seconds
		
		countDownStart = time.time()
		while int(sub_messages) < int(pub_messages) * num_pub_thread:
			time.sleep(0.5)
			print("Sent: %d, Received: %d. Waiting for complete" %(pub_messages,sub_messages))
			if (time.time() - countDownStart) > 30:
				print("Time out")
				break
		res = ("Thread: %s,  Messages Generated: %s,  Published: %s,  Received: %s, Elapsed Time: %s\n" 
		%(str(threading.get_ident()),gen_messages,pub_messages,sub_messages,str(time.time() - start)))
		# res = "Thread: " + str(threading.get_ident()) + ",  Messages Generated: " + str(gen_messages) 
		# + ",  Published: " + str(pub_messages) + ",  Received: " + str(sub_messages) 
		# + ", Elapsed Time: " + str(time.time() - start)+'\n'
		try:
			f = open(filename,'a')
			f.write(res)
			f.close()
		except Exception as e:
			print(e)
		
		print(res)
		sub_messages = 0
		pub_messages = 0
	queuep.put("stop")
		
	print("Disconnecting...")
	mqttc.disconnect()
	return True

def main():

	global sub_messages, pub_messages, the_topic_root
	
	# Content structure for configuration file "mqtt_stress_test.ini"
	#
	# [MQTT]
	# url: "localhost:1883"
	# [MESSAGES]
	# msg_num   : "100"
	# sleep_min : "0.01"
	# sleep_max : "0.50"
	# threads   : "50"
	# topic     : "/stress"
	#


	for i in range(0, num_pub_thread):
		print("Ceating thread #%s" %i)
		t = threading.Thread(target=MQTT_Client, args=())
		t.start()
		myThr.append(t)
	time.sleep(3)
	while True:
		count = 0
		for i in myThr:
			if i.is_alive():
				count += 1
		if count == 0:
			break
	
	print("Testing Done")

		
main()
