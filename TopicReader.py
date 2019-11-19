
import csv
import logging

import WorkSpawnerConfig

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Class to read in topics for pub/sub architecture
class Topics:
	"""
	Abstraction to read and cache topics for prioritization queues
	"""

	# column headings in the csv that are used to read a value from a dictionary
	topic_root_tag = 'topic root'
	topic_uid_tag = 'topic uid'
	priority_id_tag = 'priority id'
	low_score_tag = 'low score'
	high_score_tag = 'high score'

	def __init__(self, filename=None):
		"""
		Load in topics and priorities from a default file
		:param filename: override the default file name with this fully qualified name
		"""
		self.rows = []  # contains all of the rows from the config file
		self.topics = []  # save a list of the topics
		try:
			self.topic_file = WorkSpawnerConfig.TOPIC_FILE  # if defined in config file use it
		except NameError:
			self.topic_file = None

		self.priority_topic_name = WorkSpawnerConfig.priority_topic_name
		self.failed_work_topic_name = WorkSpawnerConfig.failed_work_topic_name

		if filename is None:
			self.topics = self.load_topic_file(self.topic_file)  # load default topics list.
		else:
			self.topics = self.load_topic_file(filename)  # load default topics list.

	def load_topic_file(self, filename="PubSubTopics.csv"):
		"""
		Force the loading of the topics from a specific file
		:param filename: fully qualified file name where topics are stored
		:return: list of topics in the file, None if can't be loaded
		"""
		if self._load_topic_file_rows(filename):
			return self.get_topic_list()
		else:
			logging.error('No topics found for file: ', filename)
			return None

	def _load_topic_file_rows(self, filename):
		with open(filename, newline='') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				self.rows.append(row)
		return True

	def get_topic_list(self, include_topic_root=False):
		"""

		:param include_topic_root: set to True if the fully qualified list of topics is needed.
				otherwise, the short version is returned
		:return: a list of topics.  List will be empty if none found
		"""

		# assume the column heading strings are  are:
		# Empty, topic root, topic uid, priority id, low score, high score
		#	topic root: the contains to fully qualified path to the topic including project id
		#	topic id: the name of the topic where message will be published
		#	priority id: the integer priority.  this is used in excel file to construct the topic id
		#	low score: the lowest score (inclusive) to put into the topic
		#	high score: less than this score to put into the topic

		self.topics = []  # reload all of the topics
		topic_root = ''

		# loop through all of the rows of the file and use column headings to pull out values
		for row in self.rows:
			if include_topic_root:  # if long version is desired, prepend all topics with the root
				topic_root = row.get(self.topic_root_tag)
			topic_id = row.get(self.topic_uid_tag)
			topic = topic_root + topic_id
			self.topics.append(topic)

		return self.topics

	def get_topic(self, score, include_topic_root=False):
		"""
		Retrieves the appropriate topic based on a score.  If a score is not found, the lowest priority topic is
		returned
		:param score: float value to look up.  will find the topic that includes this score in its priority list
		:param include_topic_root: set to True if the fully qualified list of topics is needed.
				otherwise, the short version is returned
		:return:
		"""
		"""get_topic retrieves the appropriate topic based on a score
			score: float value to look up
			returns: the fully qualified topic"""

		topic_root = ""
		topic = ""

		# loop through each row in memory
		for row in self.rows:
			if include_topic_root:
				topic_root = row.get(self.topic_root_tag)

			topic_id = row.get(self.topic_uid_tag)
			bin_low = float(row.get(self.low_score_tag))
			bin_high = float(row.get(self.high_score_tag))
			# if score is in the defined range for the topic, return the topic
			if bin_low <= score < bin_high:
				topic = topic_root + topic_id
				return topic

		return topic 	# will default to returning the lowest priority topic as a catch all

	def get_priority_topic(self):
		return self.priority_topic_name

	def get_failed_work_topic(self):
		return self.failed_work_topic_name


# For testing code only
if __name__ == "__main__":

	tr = Topics()
	tr.load_topic_file()

	print(tr.rows)

	topics = tr.get_topic_list()
	print(topics)

	score = 2.7

	prioritized_topic = tr.get_topic(score)
	print(prioritized_topic)
