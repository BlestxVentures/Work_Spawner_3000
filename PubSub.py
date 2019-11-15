
import logging
import datetime

# cloud imports

from google.api_core.exceptions import DeadlineExceeded
from google.api_core.exceptions import NotFound

from google.cloud import pubsub_v1

# WorkSpawner specific
import WorkSpawnerConfig
import WorkSpawner

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

	def __init__(self, platform_specific_message):
		self.platform_message = platform_specific_message
		self.body = platform_specific_message.data.decode('utf-8')
		self.attributes = dict(platform_specific_message.message.attributes)

	# this is required method because used in error handling and reporting
	def __repr__(self):
		attr_string = ""
		for key in self.attributes:
			attr_string += str(', attr_key:' + str(key) + ' ' + str(self.attributes[key]))

		repr_string = 'message: ' + str(self.body)
		repr_string += attr_string
		return repr_string

	def add_error_to_attributes(self, error_str):
		"""
		add the error string to the message attributes so can see on failed work queue
		:param error_str: string of what went wrong
		:return: none
		"""
		key = 'error_' + str(datetime.time())
		self.attributes[key] = error_str

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

	def log_failed_work(self, message):
		logging.error('Work failed for message: ' + message)


	def ack(self, message):
		"""
			acknowledges successfully processed messages

			:param ack_ids: list of ids that need to be acknowledged

		# response[].received_messages
		# received_message.ackID
		# received_message.message
		# message.data: string
		# message.attributes { string: string }
		# message.messageID: string
		# message.publishTime: string
		ack_ids = []
		for received_message in response.received_messages:
			print("Received: {}".format(received_message.message.data))
			ack_ids.append(received_message.ack_id)

		# Acknowledges the received messages so they will not be sent again.
		self.subscriber.acknowledge(subscription_path, ack_ids)
		"""
		pass


class PubSub_GCP(PubSub):

	def __init__(self):

		# for publishing
		self.publisher = pubsub_v1.PublisherClient()

		# for subscribing
		self.ack_paths = {}  # used to keep the ack_id's for successfully processed messages
		self.subscriptions = {}  # every topic requires a subscription object to interact with it.
		self.project_id = WorkSpawnerConfig.project_id
		self.subscriber = pubsub_v1.SubscriberClient()

	def get_subscription(self, topic):

		logging.debug("Looking up subscriptions for topic: " + topic)
		# see if have already looked up the subscription
		try:
			subscription_path = self.subscriptions[topic]
			return subscription_path
		except KeyError:
			pass  # continue to the rest of the function

		# assume there is a subscription with the same name as the topic
		subscription_path = self.subscriber.subscription_path(self.project_id, topic)
		logging.debug("subscription_path: " + subscription_path)

		self.subscriptions[topic] = subscription_path

		return subscription_path

	def publish(self, topic, message):
		""" Publish a message body and attributes to a topic in a PubSub environment
		:param topic: 	topic string specific to the cloud platform.  the path will be added to it
						For GC: projects/project_id/topics/topic_name
		:param message: the platform specific message to publish
		:return: True if successful, False otherwise
		"""

		logging.debug('publishing message: ' + message)

		# create the full unique path of the topic based on the current project
		topic_path = self.publisher.topic_path(self.project_id, topic)
		logging.debug('publishing on topic: ' + topic_path)

		# When a message is published a message, the client returns a "future".
		# this is to handle async responses for errors.
		# https://googleapis.dev/python/pubsub/latest/publisher/api/futures.html

		# data must be a byte string.
		payload = message.body.encode('utf-8')
		if not message.attributes:
			logging.debug('attributes are empty')
		future = self.publisher.publish(topic_path, data=payload, attributes=message.attributes)
		logging.debug(future.result())

	def pull(self, topic, max_message_count=1):

		messages = []
		# The subscriber pulls a specific number of messages.
		subscription_path = self.get_subscription(topic)
		# TODO: handle non-existent queue

		try:
			response = self.subscriber.pull(subscription_path, max_messages=max_message_count)
		except DeadlineExceeded:  # deadline is set at the subscription level
			return messages  # should be empty
		except NotFound:
			logging.error('subscription path does not exist: ' + subscription_path)
			exit(-1)

		for received_message in response.received_messages:
			ack_id = received_message.ack_id
			self.ack_paths[received_message.message.messageID] = {'path': subscription_path, 'ack_id': ack_id }
			logging.debug("Received: " + received_message)
			messages.append(Message(received_message))

		return messages

	def ack(self, message):
		# response[].received_messages
		# received_message.ackID
		# received_message.message
		# message.data: string
		# message.attributes { string: string }
		# message.messageID: string
		# message.publishTime: string

		subscription_path, ack_id = self.ack_paths[message.messageID]
		# Acknowledges the received messages so they will not be sent again.
		self.subscriber.acknowledge(subscription_path, ack_id)
		logging.debug('Acknowledged: ' + message)

	def log_failed_work(self, message):
		self.publish(WorkSpawnerConfig.failed_work_topic_name, message)


class PubSubFactory:

	@staticmethod
	def get_queue():
		if WorkSpawnerConfig.TEST_MODE:
			pubsub = PubSub()
		else:
			pubsub = PubSub_GCP()

		return pubsub
