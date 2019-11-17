
import logging
import datetime

# WorkSpawner specific
import WorkSpawnerConfig

# logging format is set in the WorkSpawnerConfig...this changes the level in this file.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


#  This is effectively an abstract base class that is platform independent
#  and encapsulates the information and interface that is passed around on the message queue
#  the dummy implementation is only for testing
class Message:

	def __init__(self, body, attributes):
		"""
		:param attributes: dict of things passed along with the message in the queue
		:param body: binary blob of data
		"""
		self.body = body
		self.attributes = attributes
		self.acknowledged = False

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

	# over ride this method for platform specific work
	def ack(self):
		self.acknowledged = True  # whether message has been acknowledge to pub/sub queue


#  This is effectively an abstract base class that is platform independent
#  it encapsulates platform independent PubSub information and interface needed
#  to participate in the WorkSpawner and Prioritizer
class PubSub:  # base class that describes the implementation independent interface

	# override this method with platform specific init
	def __init__(self):
		self.queue = {}  # a dictionary of all of the topics the PubSub will communicate with

	# override this method with platform specific methods
	def publish(self, topic, message):
		"""
		publish message on topic
		:param topic: short topic name to publish message
		:param message: message to publish
		:return: success = True, False otherwise
		"""
		try:
			self.queue[topic]  # if a queue hasn't been created yet, create one
		except KeyError:
			self.queue[topic] = []

		self.queue[topic].append(message)

		# for debugging only
		debug_msg = 'Queuing-> ' + message + ' to topic: ' + str(topic)
		logging.debug(debug_msg)
		return True

	# override this method with platform specific methods
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

	# override this method with platform specific methods
	def log_failed_work(self, message):
		logging.error('Work failed for message: ' + str(message))

	# override this method with platform specific methods
	def ack(self, message):
		"""
			acknowledges successfully processed messages
			:param message: message to acknowledge
		"""
		message.ack()


# ----  Google Cloud Platform implementation below here -----
# cloud specific imports
from google.api_core.exceptions import DeadlineExceeded
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1


class Message_GCP(Message):
	"""
	GCP specific version of Message
	"""
	def __init__(self, body='', attributes=None):
		self.body = body
		self.attributes = attributes
		self.received_message = None  # used to store the full message received if any

	def create_from_received_message(self, received_message):
		self.received_message = received_message  # this has other data stored with it.
		self.body = self.received_message.message.data.decode('utf-8')
		self.attributes = dict(self.received_message.message.attributes)
		logging.debug('created a message: ' + str(self))  # base class repr should be able to print this

	def _convert_attributes(self):
		"""GCP Pubsub requires attributes to be strings when published.  This converts them"""
		ret_attribs = {}

		for key in self.attributes:
			logging.debug('Type of attribute: ' + str(key) + ' is: ' + type(self.attributes[key]).__name__)
			ret_attribs[key] = str(self.attributes[key])

		return ret_attribs


class PubSub_GCP(PubSub):

	def __init__(self):

		# for publishing
		self.publisher = pubsub_v1.PublisherClient()

		# for subscribing
		self.subscriber = pubsub_v1.SubscriberClient()
		self.ack_paths = {}  # used to keep the ack_id's for successfully processed messages
		self.subscriptions = {}  # every topic requires a subscription object to interact with it.

		# store project id from the configuration file
		self.project_id = WorkSpawnerConfig.project_id

	def _get_subscription(self, topic):
		"""
		private method only used by GCP
		:param topic: short name of topic to lookup the subscription for.
		:return: subscription_path as a string
		"""

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

		logging.debug('publishing message: ' + str(message))

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

		if isinstance(message, Message_GCP):
			# if a Message_GCP, then use function to convert it.  Otherwise assume attribs are strings
			logging.debug('Converting attributes to string: ' + str(message))
			attribs = message._convert_attributes()


		future = self.publisher.publish(topic_path, data=payload, **attribs)
		logging.debug(future.result())

	def pull(self, topic, max_message_count=1):
		"""
		poll to see if there is are any messages for the given topic
		:param topic: short name for the topic
		:param max_message_count: number of messages to pull if available, will pull up to max
		:return: list of messages pulled, empty if non available
		"""

		messages = []
		response = None

		# use the topic to find the appropriate subscription
		subscription_path = self._get_subscription(topic)

		# The subscriber attempts to pull all of the messages up to the max.
		try:
			response = self.subscriber.pull(subscription_path, return_immediately=True, max_messages=max_message_count)
		except DeadlineExceeded:  # deadline is set at the subscription level.  if no work available by deadline, return
			return messages  # should be empty
		except NotFound:
			logging.error('subscription path does not exist: ' + subscription_path)
			exit(-1)

		# response: definition google.cloud.pubsub_v1.types.PullResponse
		# received_messages will be empty if none are available
		# received_message: definition google.cloud.pubsub_v1.types.ReceivedMessage

		logging.debug('type of response received: ' + type(response).__name__)

		for received_message in response.received_messages:
			logging.debug('type of message received: ' + type(received_message).__name__)
			ack_id = received_message.ack_id
			self.ack_paths[received_message.message.message_id] = {'path': subscription_path, 'ack_id': ack_id}
			logging.debug("Received message: " + str(received_message))
			message = Message_GCP()
			message.create_from_received_message(received_message)
			messages.append(message)

		return messages

	def ack(self, message):
		# response[].received_messages
		# received_message.ackID
		# received_message.message
		# message.data: string
		# message.attributes { string: string }
		# message.messageID: string
		# message.publishTime: string

		# Acknowledges the received messages so they will not be sent again.

		try:  # if came from a received message, should have ack() method on it.
			message.received_message.ack()  # Python PubsubMessage has a method to ack itself
			logging.debug('Acknowledged using built in ack method: ' + str(message))
			return
		except Exception:  # try try again
			logging.debug('no ack method on received_message')

		message_id = message.received_message.message.message_id
		r_ack_id = message.received_message.ack_id
		subs = self.ack_paths[message_id]
		subscription_path = subs['path']
		ack_ids = [subs['ack_id']]

		logging.debug('subscription path to ack: ' + str(subscription_path))
		logging.debug('received message ack_id: ' + str(r_ack_id))
		logging.debug('message_id: ' + str(message_id) + ' ack_id: ' + str(ack_id))
		self.subscriber.acknowledge(subscription_path, ack_id)
		logging.debug('Acknowledged using explicit acknowledge: ' + str(message))

	def log_failed_work(self, message):
		self.publish(WorkSpawnerConfig.failed_work_topic_name, message)


# ---- Used to abstract the instantiation of the platform specific class ----
class PubSubFactory:

	@staticmethod
	def get_queue():
		if WorkSpawnerConfig.TEST_MODE:
			pubsub = PubSub()
		else:
			pubsub = PubSub_GCP()

		return pubsub
