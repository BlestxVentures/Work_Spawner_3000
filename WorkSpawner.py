#
# Work Spawner 3000 code
#
import signal
import sys
import time
from subprocess import Popen
from datetime import datetime
import logging
import argparse

#  Local modules
import WorkSpawnerConfig
import TopicReader

#  This is the module that contains all of the domain specific work.
import MyWork

# logging format is set in the WorkSpawnerConfig...this changes the level in this file.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


#  This class encapsulates the information that is passed around on the message queue
class Message:

	def __init__(self, body, attributes):
		"""
		:param attributes: dict of things passed along with the message in the queue
		:param body: binary blob of data
		"""
		self.body = str(body)
		self.attributes = attributes
		self.acknowledged = False

	def __repr__(self):
		attr_string = ""
		for key in self.attributes:
			attr_string += str(', attr_key:' + str(key) + ' ' + str(self.attributes[key]))

		repr_string = 'message: ' + str(self.body)
		repr_string += attr_string
		return repr_string

	def ack(self):
		#TODO: will need the topic so can make sure it is popped off.
		self.acknowledged = True  # whether message has been acknowledge to pub/sub queue


#  This class encapsulates the PubSub functionality needed
class PubSub:  # base class that describes the implementation independent interface

	def __init__(self):
		self.queue = {}  # a dictionary of all of the topics the PubSub will communicate with

	def publish(self, topic, body, attributes):
		"""	topic: the topic to which message will be published
			body: assumed binary data of message to pass to/from work
			attributes: dictionary list of attributes to send along with the body
		"""
		try:
			self.queue[topic]  # if a queue hasn't been created yet, create one
		except KeyError:
			self.queue[topic] = []

		message = Message(attributes, body)
		self.queue[topic].append(message)

		# for debugging only
		debug_msg = 'Queuing-> ' + str(message) + ' to topic: ' + str(topic)
		logging.debug(debug_msg)

	def pull(self, topic, max_message_count=1):
		"""	topic: the topic to pull a message from
			max_message_count: how many messages to process in a given call
		"""
		try:
			self.queue[topic]  # see if there is a queue for a topic
		except KeyError:
			return None  # no such topic

		messages = self.queue[topic]  # list of messages for this topic

		messages_to_return = messages[:max_message_count]

		# for debugging only
		debug_msg = ''
		for message in messages_to_return:
			debug_msg += 'DeQueuing-> ' + str(message) + ' from topic: ' + str(topic)

		logging.debug(debug_msg)

		return messages[:max_message_count]  # return a subset of those messages

	def ack(self, ack_ids):
		"""
			acknowledges successfully processed messages

			:param ack_ids: list of ids that need to be acknowledged

		ack_ids = []
		for received_message in response.received_messages:
			print("Received: {}".format(received_message.message.data))
			ack_ids.append(received_message.ack_id)

		# Acknowledges the received messages so they will not be sent again.
		self.subscriber.acknowledge(subscription_path, ack_ids)
		"""
		pass


class Spawner:

	def __init__(self):
		self.subprocess = None

	def pre_process(self, message):  # things that need to be done before processing work
		MyWork.pre_process(message)

	def post_process(self, message):  # things that need to be done after the work is complete
		MyWork.post_process(message)

	def get_work_cmd(self, message):
		return MyWork.get_work_cmd(message)

	def spawn_docker(self, docker_id, body, attributes):
		cmd = ['docker', 'run', '--rm', docker_id]
		logging.debug('Docker cmd: ' + str(cmd))
		self.subprocess = Popen(cmd)

	def spawn_shell(self, message):
		"""	payload: gets passed to the process"""
		cmd = self.get_work_cmd(message)

		logging.debug('shell cmd: ' + str(cmd))
		self.subprocess = Popen(cmd)  # default hook to start work.
		logging.info('spawned subprocess: ' + str(self.subprocess.pid))

	def wait(self, timeout):
		"""wait for a subprocess to be done or it times out
		:param timeout: number of seconds to wait for work to be done, otherwise stop. if zero, will wait forever
		:return: exitcode of the subprocess or -1 if timed out
		"""
		if timeout:
			tracking_timeout = True
			timeout_ctr = timeout
		else:
			tracking_timeout = False
			timeout_ctr = 0

		process_done = False

		while not process_done:
			timeout_ctr -= 1  # decrement the timeout counter
			time.sleep(1)

			rc = self.subprocess.poll()  # returns None if not done, else returns error code from subprocess
			if rc is None:  # process isn't done
				continue  # the while loop
			else:  # process completed and returned a return code
				exitcode = rc
				process_done = True

			if tracking_timeout and timeout_ctr <= 0:
				exitcode = self.subprocess.terminate()
				if not exitcode:  # even if successfully terminated, return an error due to time out
					return -1

		return exitcode


# read used to assign work to different priority queues
class Prioritizer:

	def __init_(self):
		pass

	def prioritize(self, message):
		return MyWork.prioritize(message)


#  Class to load the configuration for the pub/sub topics
class CloudStore:  # defines implementation independent interface

	def __init__(self):
		self.topic_reader = TopicReader.Topics()
		self.topics = []

	def get_topics(self):  # needs to always reload the topics
		if WorkSpawnerConfig.TOPIC_FILE:  # look for the default location for list of topics
			self.topics = self.topic_reader.load_topic_file(WorkSpawnerConfig.TOPIC_FILE)
		else:  # default case for testing
			self.topics = ['topic-1', 'topic-2', 'topic-3', 'topic-4', 'topic-5']

		# ordered list of topics
		return self.topics

	def get_topic(self,score):
		return self.topic_reader.get_topic(score)


def work_spawner(test=False):

	def signal_handler(sig, frame):
		logging.info('work_spawner is being terminated')
		sys.exit(0)

	# handle CTRL-C to stop subprocess
	signal.signal(signal.SIGINT, signal_handler)

	spawner = Spawner()
	queue = MyWork.PubSubFactory.get_cloud_specfic()
	store = CloudStore()

	# always load the topics in case they have changed
	# topics are arranged highest to lowest
	topics = store.get_topics()
	index = 0  # index into the list of topics

	if test:  # if in test mode put some dummy data on the queue
		for topic in topics:
			attributes = {1: "attr1", 2: "attr2"}
			body = "sample body text"
			queue.publish(topic, body, attributes)

	while True:

		if index >= len(topics):  # must have gone through all of the topics without finding work
			logging.info("No work found")
			index = 0  # reset the index for next time checking for work
			time.sleep(10)  # if reached the end of the topics and there was no work, then sleep for a while
			continue  # restart the while loop

		# Get the next topic from a list of topics
		topic = topics[index]  # constrains the index to be inside of topics
		logging.debug('Topic being used: ' + topic)

		messages = queue.pull(topic, 1)

		if not messages:  # if there are no messages on that queue, move to next one.
			index += 1  # Move to lower priority topic if no message
			continue
		else:  # If we received and processed a message, reset back to the highest priority topic
			index = 0

		# If we got any messages, spawn a subprocess to handle each message in order received
		# then start over with the highest priority topic again
		for message in messages:  # loop through all of the messages and process each one
			logging.debug('message: ' + str(message.body) + ' pulled from: ' + str(topic))

			# perform any work that needs to be done before spawned. e.g., copying files etc.
			spawner.pre_process(message)

			# if there is a docker_id in the attributes, use it to spawn a docker file
			if 'docker_id' in message.attributes:
				docker_id = message.attributes['docker_id']
				del message.attributes['docker_id']

				# spawn as a sub process
				spawner.spawn_docker(docker_id, message.body, message.attributes)
			else:
				# spawn as a shell process
				spawner.spawn_shell(message)

			# wait for the subprocess to error or time out
			if spawner.wait(WorkSpawnerConfig.WAIT_TIMEOUT):  # wait this many seconds at most to finish
				logging.error('worker errored or timedout')
			else:
				message.ack()  # only acknowledge the message if successfully processed
				logging.debug('work finished successfully')

			spawner.post_process(message)


def work_prioritizer(testing):
	def signal_handler(sig, frame):
		logging.info('work_prioritizer is being terminated')
		sys.exit(0)

	# handle CTRL-C to stop subprocess
	signal.signal(signal.SIGINT, signal_handler)

	prioritizer = Prioritizer()
	queue = MyWork.PubSubFactory.get_cloud_specfic()
	store = CloudStore()

	# always load the topics in case they have changed
	# topics are arranged highest to lowest
	topics = store.get_topics()

	priority_topic = WorkSpawnerConfig.priority_topic_name

	while True:

		# pull next work to prioritize
		logging.debug('Pulling work from priority_topic: ' + priority_topic)
		messages = queue.pull(priority_topic, 1)

		if not messages:  # if there are no messages on that queue, move to next one.
			logging.debug('no work found on prioritization queue')
			time.sleep(10)
			continue

		# If we got any messages
		for message in messages:  # loop through all of the messages and process each one
			logging.debug('message: ' + str(message.body) + ' pulled from: ' + str(priority_topic))

			# perform any work that needs to be done before spawned. e.g., copying files etc.
			score = prioritizer.prioritize(message)
			topic_to_publish_on = store.get_topic(score)
			if topic_to_publish_on:
				queue.publish(topic_to_publish_on, message.body, message.attributes)
			else:
				logging.error('could not find a topic to send work to for score: ' + str(score))


if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("--spawner", help="run the work spawner daemon", action="store_true")
	parser.add_argument("--prioritizer", help="run the work prioritizer daemon", action="store_true")
	parser.add_argument("--test", help="generate dummy data test pub sub", action="store_true")
	parser.add_argument("--simulate", help="simulate pubsub infrastructure in memory to test spawned work", action="store_true")

	# get the args
	args = parser.parse_args()

	testing = args.test
	if testing:  # testing mode will generate and process fake data to test pub sub infrastructure
		logging.debug('In test mode')
		WorkSpawnerConfig.TEST_MODE = True  # set the global state

	if args.spawner:
		work_spawner(WorkSpawnerConfig.TEST_MODE)
	elif args.prioritizer:
		work_prioritizer(WorkSpawnerConfig.TEST_MODE)
	else:
		logging.error("Need to specify --spawner or --prioritizer")

