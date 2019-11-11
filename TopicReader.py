
import csv


# Class to read in topics for pub/sub architecture
class Topics:

	topic_root_tag = 'topic root'
	topic_uid_tag = 'topic uid'
	priority_id_tag = 'priority id'
	low_score_tag = 'low score'
	high_score_tag = 'high score'

	def __init__(self):
		self.rows = []  # contains all of the rows from the config file
		self.topics = []  # save a list of the topics

	def load_topic_file(self, filename="PubSubTopics.csv"):
		if self._load_topic_file_rows(filename):
			return self.get_topic_list()
		else:
			return None

	def _load_topic_file_rows(self, filename):
		with open(filename, newline='') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				self.rows.append(row)
		return True

	def get_topic_list(self):

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
			# topic_root = row.get(self.topic_root_tag)  get without the root for now
			topic_id = row.get(self.topic_uid_tag)
			topic = topic_root + topic_id
			self.topics.append(topic)

		return self.topics

	def get_topic(self, score):
		"""get_topic retrieves the appropriate topic based on a score
			score: float value to look up
			returns: the fully qualified topic"""

		topic_root = ""  # default to no root for now.  topic_root should be the project id
							# TODO: lookup the project id and build string
							# e.g., "projects/work-spawner-3000/topics/"

		for row in self.rows:
			# topic_root = row.get(self.topic_root_tag) get without the root for now
			topic_id = row.get(self.topic_uid_tag)
			bin_low = float(row.get(self.low_score_tag))
			bin_high = float(row.get(self.high_score_tag))
			if bin_low <= score < bin_high:
				topic = topic_root + topic_id
				return topic

		return topic_root + "CatchAllLowestPriority" 	# default name in case a topic isn't found.


if __name__ == "__main__":

	tr = Topics()

	tr.load_topic_file()

	print(tr.rows)

	topics = tr.get_topic_list()
	print(topics)

	score = 2.7

	prioritized_topic = tr.get_topic(score)
	print(prioritized_topic)

