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
logger.setLevel(logging.INFO)


# stateless re-entrant functions
def pre_process(message):  # things that need to be done before processing work
	logging.debug('pre_processing: ' + str(message))
	# payload definition
	# parse starter config and starter genome

	# src directory for the config and starter genomes
	# copy all files from src directory to ./config/
	bucket = WorkSpawnerConfig.DEFAULT_BUCKET_NAME
	src_file = 'gs://' + bucket + '/config/*'
	dest_file = '../Bug-World/config/'
	cmd = ['gsutil', 'cp', '-r', src_file, dest_file ]
	logging.info('executing the following command: ' + str(cmd))

	try:
		rv = subprocess.call(cmd)
		logging.debug('command completed without exception')
		if not rv:
			rv = True
		else:
			rv = False
	except:
		logging.error('command threw an exception')
		rv = False

	logging.debug('returning: ' + str(rv))
	return rv  # if everything was successful


def post_process(message):  # things that need to be done after the work is complete
	"""
	:param message: PubSub message to that was used for processing
	:return: True if everything was successful
	"""

	logging.debug('post_processing: ' + str(message))

	# unpack the payload and do any work that needs to be done
	# get machine name
	# construct destination directory root
	# use gsutils to mv all directories from ./logs/*
	#https://cloud.google.com/storage/docs/gsutil/commands/cp

	bucket = WorkSpawnerConfig.DEFAULT_BUCKET_NAME 
	base_path = 'gs://' + bucket + '/Bug-World/logs/'
	cmd = ['gsutil', 'mv', '../Bug-World/logs/', base_path]
	logging.info('executing the following command: ' + str(cmd))
	try:
		rv = subprocess.call(cmd)
		logging.debug('command completed without exception')
		if not rv:
			rv = True
		else:
			rv = False
	except:
		logging.error('command threw an exception')
		rv = False

	if rv:
		logging.debug('command was successful')
		# if the copy worked, prioritize the work for next loop
		priority_message = 'Prioritize this: ' + base_path
		q = PubSub.PubSubFactory.get_queue()
		message = PubSub.Message_GCP(priority_message)
		q.publish(WorkSpawnerConfig.priority_topic_name, message)

	logging.debug('returning: ' + str(rv))
	return rv  # if everything was successful


def get_work_cmd(message):  # default stub
	logging.debug('work command for: ' + str(message))

	# unpack the payload and do any work that needs to be done
	#cmd_to_run = ['python', 'MyWork.py', '--test']  # needs to be something Popen can run.
	#cmd_to_run = ['python', 'MyWork.py']  # needs to be something Popen can run.
	cmd_to_run = ['python', 'main.py', '--nodisplay']  # needs to be something Popen can run.

	cwd = '../Bug-World'
	logging.debug('cmd: ' + str(cmd_to_run ) + ' in dir: ' + cwd )

	return cmd_to_run, cwd


def prioritize(message):  # where the prioritization happens based on the message
	logging.debug('prioritizing: ' + str(message))
	if 'priority' in message.attributes:
		rand_int = int(message.attributes['priority'])
		logging.debug('returning a priority based on an attribute')
	else:
		rand_int = random.randint(1, 10)
		logging.debug('generating a random score of: ' + str(rand_int))
	return rand_int


if __name__ == "__main__":

	logging.info('Started the work')
	parser = argparse.ArgumentParser()
	parser.add_argument("--test", help="run and generate dummy data to work on", action="store_true")

	args = parser.parse_args()

	if args.test:
		logging.info("Using Test Work")
		time.sleep(1)  # work for a minute
		exit(0)  # exit successfully

