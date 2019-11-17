# standard imports
import logging
import argparse
import subprocess

# for user specific work
import time
import random

# WorkSpawner specific
import WorkSpawnerConfig
import PubSub
from PubSub import Message

# Specific to MyWork
import MyWorkConfig

# logging format is set in the WorkSpawnerConfig...this changes the level in this file.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# stateless re-entrant functions
def pre_process(message):  # things that need to be done before processing work
	logging.debug('pre_processing: ' + str(message))
	# payload definition
	# src directory for the config and starter genomes
	# copy all files from src directory to ./config/*
	cmd = ['gsutil', 'cp', '-r', 'gs://ws-proto-bucket-1/Bug-World/*', '.']

	logging.info('executing the following command: ' + str(cmd))

	rv = True
	try:
		rv = subprocess.call(cmd)
		if not rv:
			return True
	except:
		rv = False

	return rv  # if everything was successful


def post_process(message):  # things that need to be done after the work is complete
	logging.debug('post_processing: ' + str(message))

	# unpack the payload and do any work that needs to be done
	# get machine name
	# construct destination directory root
	# use gsutils to mv all directories from ./logs/*
	#https://cloud.google.com/storage/docs/gsutil/commands/cp
	base_path = "gs://ws-proto-bucket-1/Bug-World/"
	curr_time = str(time.time())
	sim_run_path = base_path + curr_time
	cmd = ['gsutil', 'mv', './logs/*', sim_run_path]
	logging.info('executing the following command: ' + str(cmd))
	rv = True
	try:
		rv = subprocess.call(cmd)
		if not rv:
			return True
	except:
		rv = False

	return rv  # if everything was successful



def get_work_cmd(message):  # default stub
	logging.debug('work command for: ' + str(message))

	# unpack the payload and do any work that needs to be done
	#cmd_to_run = ['python', 'MyWork.py', '--test']  # needs to be something Popen can run.
	cmd_to_run = ['python', 'MyWork.py']  # needs to be something Popen can run.
	logging.debug('cmd: ' + str(cmd_to_run ))

	return cmd_to_run


def prioritize(message):  # where the prioritization happens based on the message
	logging.debug('prioritizing: ' + str(message))
	if 'priority' in message.attributes:
		rand_int = int(message.attributes['priority'])
		logging.debug('returning a priority based on an attribute')
	else:
		rand_int = random.randint(1, 10)
		logging.debug('generating a random score of: ' + str(rand_int))
	return rand_int


import ConceptTester
if __name__ == "__main__":

	logging.info('Started the work')
	parser = argparse.ArgumentParser()
	parser.add_argument("--test", help="run and generate dummy data to work on", action="store_true")

	args = parser.parse_args()

	if args.test:
		logging.debug("Using Test Work")
		time.sleep(1)  # work for a minute
		print('Here is what it is before: ', WorkSpawnerConfig.TEST_MODE)
		r = ConceptTester.test_that_concept_thangy(WorkSpawnerConfig.TEST_MODE)
		print('Here is what what passed back', r)
		print('Here is what it is now: ', WorkSpawnerConfig.TEST_MODE)
		exit(0)  # exit successfully

	logging.debug("Using normal Work")

	log_file = './logs/file' + str(time.time())
	with open(log_file, "a") as f:
		f.write("time 1: " + str(time.time()))
		time.sleep(10)
		f.write("time 2: " + str(time.time()))

	priority_message = 'Prioritize this: ' + log_file
	q = PubSub.PubSubFactory.get_queue()
	message = PubSub.Message_GCP(priority_message)
	q.publish(WorkSpawnerConfig.priority_topic_name, message)

	exit(0)
