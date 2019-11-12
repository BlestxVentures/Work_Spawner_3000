# cloud imports
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import pubsub_v1

# WorkSpawner specific
import WorkSpawnerConfig
import WorkSpawner

class PubSub_GCP:

	def __init__(self):

		# for publishing
		self.publisher = pubsub_v1.PublisherClient()

		# for subscribing
		self.ack_ids = []  # used to keep the ack_id's for successfully processed messages
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

	def publish(self, topic, body, attributes):
		""" Publish a message body and attributes to a topic in a PubSub environment
		:param topic: 	topic string specific to the cloud platform.  the path will be added to it
						For GC: projects/project_id/topics/topic_name
		:param body: binary blob of data
		:param attributes: dictionary of custom attributes to pass along with message
		:return: True if successful, False otherwise
		"""

		#logging.debug('publishing body: ' + str(body) + ' attributes: ' + attributes)

		# create the full unique path of the topic based on the current project
		topic_path = self.publisher.topic_path(self.project_id, topic)
		logging.debug('publishing on topic: ' + topic_path)

		# When a message is published a message, the client returns a "future".
		# this is to handle async responses for errors.
		# https://googleapis.dev/python/pubsub/latest/publisher/api/futures.html

		# data must be a byte string.
		payload = body.encode('utf-8')
		if not attributes:
			logging.debug('attributes are empty')
		future = self.publisher.publish(topic_path, data=payload, attributes=attributes)
		logging.debug(future.result())

	def pull(self, topic, max_message_count=1):

		messages = []
		# The subscriber pulls a specific number of messages.
		subscription_path = self.get_subscription(topic)

		try:
			response = self.subscriber.pull(subscription_path, max_messages=max_message_count)
		except DeadlineExceeded:
			return messages

		for received_message in response.received_messages:
			logging.debug("Received: {}".format(received_message.message.data))
			self.ack_ids.append(received_message.ack_id)
			payload = received_message.message.data.decode('utf-8')
			attributes = dict(received_message.message.attributes)
			messages.append(WorkSpawner.Message(payload, attributes))

		# Acknowledges the received messages so they will not be sent again.
		# TODO: move this to after the message has been processed successfully
		self.subscriber.acknowledge(subscription_path, self.ack_ids)

		logging.info('Received and acknowledged {} messages. Done.'.format(
			len(response.received_messages)))

		# TODO: handle non-existent queue


		return messages


class PubSubFactory:

	@staticmethod
	def get_cloud_specfic():
		if WorkSpawnerConfig.TEST_MODE:
			pubsub = WorkSpawner.PubSub()
		else:
			pubsub = PubSub_GCP()

		return pubsub