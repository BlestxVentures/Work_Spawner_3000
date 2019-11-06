import os
import datetime

# name of the project where resources are
# TODO: use this to get project_id
#  curl "http://metadata.google.internal/computeMetadata/v1/project/project-id" -H "Metadata-Flavor: Google"
# compute = googleapiclient.discovery.build('compute', 'v1')
# https://cloud.google.com/compute/docs/reference/rest/v1/projects/get

project_id = "work-spawner-3000"

topic_name = "priority-1"  # default pub/sub topic name
subscription_name = topic_name  # default subscription name matches topic

# name of the bucket where work files will be stored long term
bucket_name = 'ws-proto-bucket-1'

# use the directory where the simulation is running
local_dir = os.path.dirname(__file__)

# where the starting config files exist and seed genomes
config_sub_dir = "config"
config_dir = os.path.join(local_dir, config_sub_dir)

# timestamp a subdirectory to store data
log_sub_dir = os.path.join("logs", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
log_dir = os.path.join(local_dir, log_sub_dir)

