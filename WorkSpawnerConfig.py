import logging

logging.basicConfig(format='%(process)d: %(asctime)s: %(levelname)s: %(funcName)s: %(message)s', level=logging.INFO)


#  the location of the topic configurator file
#  bucket_name/topicfile
#TOPIC_FILE = "ws-proto-bucket-1/PubSubtopics.csv"
TOPIC_FILE = 'PubSubTopics.csv'

# name of topic to look for work to prioritize
priority_topic_name = "work-to-prioritize"

# how long to wait for work before timing out
WAIT_TIMEOUT = 60

# should run in test mode and not use actual cloud functions
TEST_MODE = False
SIMULATION_MODE = False



# GC Project id...TODO: replace this by getting the metadata
project_id = "work-spawner-3000"
