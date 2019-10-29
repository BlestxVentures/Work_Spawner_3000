import logging

logging.basicConfig(format='%(process)d: %(asctime)s: %(levelname)s: %(funcName)s: %(message)s', level=logging.DEBUG)


#  the location of the topic configurator file
#  bucket_name/topicfile
#TOPIC_FILE = "ws-proto-bucket-1/topics.csv"
TOPIC_FILE = 'GCPTopics.csv'

# how long to wait for work before timing out
WAIT_TIMEOUT = 60
