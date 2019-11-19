
This is used to spawn work across several prioritized pub sub queues on Google Cloud Platform

Usage:
- PubSubTopics.csv - contains the topics, the priority and the range
- WorkSpawnerConfig.py - contains the necessary configuration variables
    WAIT_TIMEOUT = time in seconds to give the subprocess to finish before abandons it
    project_id = the name of the project where topics and subscriptions are stored
    topic_file = location to find the topic file to read in.  By default it is PubSubTopics.csv

Run:
--> on a vm_instance where installed and work exists, use this to pull work and spawn it
$ python3 WorkSpawner.py --spawner &

--> on any vm_instance, only need one of these to persistently run to monitor work queue and prioritize
$ python3 WorkSpawner.py --prioritize &

Setup
Required Modules:
- google-cloud
- google-api-core
- google-cloud-pubsub

PubSub Requirements:
- must create a topic for each topic listed in PubSubTopics.csv
- must create a pull subscription for each topic in PubSubTopics.csv with the same name as the topic
- must create a topic to log failed work.  This is configured in WorkSpawnerConfig.py
- must create a topic to pull work that needs to be prioritized.  This is configured in WorkSpawnerConfig.py

Configure user specific work:
MyWork.py
- pre_process: work that needs to be done before the actual process is run.  E.g., copy files
- post_process: work that needs to be done after the process has run successful.
    E.g., copy files, put more work on priority queues
- get_work_cmd: the command line that will be passed to popen to run the actual work
- prioritize: given a message from the priority queue, the function must return a score.
    The score will be looked up in PubSubTopics.csv and the appropriate topic name for that score will be used

Options:
- set the debug level in each module to the desired debug level.  default is error.
