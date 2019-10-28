#
# Work Spawner 3000 code
#
import signal
import sys
import time
from subprocess import Popen
from datetime import datetime
import logging

#  Local modules
import WorkSpawnerConfig
import TopicReader

#  This is the module that contains all of the domain specific work.
import MyWork


#  This class encapsulates the information that is passed around on the message queue
class Message:

	def __init__(self, attributes, body):
		"""	attributes: dict of things passed along with the message in the queue
			body: binary blob of data
		"""
		self.attributes = attributes
		self.body = body
		self.acknowledged = False

	def __repr__(self):
		return "attributes: " + self.attributes + "\n" + "Body: " + str(self.body)

	def ack(self):
		self.acknowledged = True  # whether message has been acknowledge to pub/sub queue


#  This class encapsulates the PubSub functionality needed for the Work Spawner
class PubSub:

	def __init__(self):
		self.queue = {}  # a dictionary of all of the topics the PubSub will communicate with

	def publish(self, topic, attributes, body):
		"""	topic: the topic to which message will be published
			attributes: dictionary list of attributes to send along with the body
			body: assumed binary data of message to pass to/from work
		"""
		if self.queue[topic] is None:
			self.queue[topic] = []

		self.queue[topic].append(Message(attributes, body))

	def pull(self, topic, max_message_count=1):
		"""	topic: the topic to pull a message from
			max_message_count: how many messages to process in a given call
		"""
		if topic in self.queue:
			return self.queue[:max_message_count]  # return a list of message for this topic
		else:
			return None


class Spawner:

	def __init__(self):
		self.subprocess = None

	def pre_process(self):  # things that need to be done before processing work
		pass

	def post_process(self):  # things that need to be done after the work is complete
		pass

	def process_work(self):  # default stub
		pass

	def spawn_docker(self, docker_id, docker_args, payload):
		self.subprocess = Popen(['docker', 'run', '--rm', docker_id].extend(docker_args))

	def spawn_shell(self, payload):
		"""	payload: gets passed to the process"""
		self.subprocess = Popen(['main.py'])

	def terminate(self, timeout=None):

		if timeout is None:
			timeout = 0  # infinite

		if self.subprocess:
			self.subprocess.terminate()

			exit_code = self.wait(timeout=10)

			if timeout <= 0:
				self.subprocess.kill()

	def wait(self, timeout):
		exitcode = None
		while not exitcode and timeout:  # returns None if the process is still running
			timeout -= 1
			time.sleep(1)
			exitcode = self.subprocess.poll()

		if timeout <= 0:
			exitcode = self.subprocesss.terminate(10)

		return exitcode


#  Class to load the configuration for the pub/sub topics
class CloudStore:

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


if __name__ == "__main__":

	def signal_handler(self, sig, frame):
		spawner.terminate(10)
		sys.exit(0)

	# handle CTRL-C to stop subprocess
	signal.signal(signal.SIGINT, signal_handler)

	spawner = Spawner()
	queue = PubSub()
	store = CloudStore()

	# always load the topics in case they have changed
	# topics are arranged highest to lowest
	topics = store.get_topics()
	index = 0  # index into the list of topics

	while True:

		if index >= len(topics):
			logging.info("No work found")
			index = 0  # reset the index for next time checking for work
			time.sleep(60)  # if reached the end of the topics and there was no work, then sleep for a while
			continue  # restart the while loop

		# Get a topic from a list of topics
		topic = topics[index]  # constrains the index to be inside of topics
		logging.debug(topic)

		message = queue.pull(topic, 1)
		logging.debug('message' + message + 'pulled from: ' + topic)

		# If we got any messages, spawn a docker container
		# When the container exits, start over with the highest
		# priority topic
		if message:
			spawner.pre_process()

			if 'docker_id' in message.attributes:
				docker_id = message.attributes['docker_id']
				del message.attributes['docker_id']

				if spawner.spawn_docker(docker_id, message.attributes, message.body):
					message.ack()  # only acknowledge the message if successfully processed

				spawner.wait(5 * 60)

			spawner.post_process()

			# If we received and processed a message, reset back to the
			# highest priority topic
			index = 0
		else:
			index += 1  # Move to lower priority topic if no message
