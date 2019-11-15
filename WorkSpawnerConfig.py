import logging

# set the default logging format and to only log errors.  logging level is overridden in each module if desired
logging.basicConfig(format='%(process)d: %(asctime)s: %(levelname)s: %(funcName)s: %(message)s', level=logging.ERROR)


#  the location of the topic configurator file
#  bucket_name/topicfile
DEFAULT_BUCKET_NAME = "ws-proto-bucket-1"
TOPIC_FILE = DEFAULT_BUCKET_NAME + "/PubSubtopics.csv"
TOPIC_FILE = 'PubSubTopics.csv'  # look in local directory for now

# name of topic to look for work to prioritize
priority_topic_name = "work-to-prioritize"
failed_work_topic_name = "failed-work"

# how long to wait for work before timing out
WAIT_TIMEOUT = 60

# should run in test mode and not use actual cloud functions, command line args can override this
TEST_MODE = False

# name of the project where resources are
# TODO: use this to get project_id
#  curl "http://metadata.google.internal/computeMetadata/v1/project/project-id" -H "Metadata-Flavor: Google"
# compute = googleapiclient.discovery.build('compute', 'v1')
# https://cloud.google.com/compute/docs/reference/rest/v1/projects/get
project_id = "work-spawner-3000"
