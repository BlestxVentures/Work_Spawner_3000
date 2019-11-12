# standard imports
import logging
import time
import argparse
import random


# cloud imports
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import pubsub_v1

# WorkSpawner specific
import WorkSpawnerConfig
import WorkSpawner


# Specific to MyWork
import MyWorkConfig

# logging format is set in the WorkSpawnerConfig...this changes the level in this file.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


#class PubSub_GCP(WorkSpawner.PubSub):


# stateless re-entrant functions
def pre_process(message):  # things that need to be done before processing work
	logging.debug(message)
	# payload definition
	# src directory for the config and starter genomes
	# copy all files from src directory to ./config/*

	return True  # if everything was successful


def post_process(message):  # things that need to be done after the work is complete
	logging.debug(message)

	# unpack the payload and do any work that needs to be done
	# get machine name
	# construct destination directory root
	# use gsutils to mv all directories from ./logs/*
	#https://cloud.google.com/storage/docs/gsutil/commands/cp
	return True  # if everything successful


def get_work_cmd(message):  # default stub
	logging.debug(message)

	# unpack the payload and do any work that needs to be done
	cmd_to_run = ['python', 'MyWork.py', '--test']  # needs to be something Popen can run.
	return cmd_to_run


def prioritize(message):  # where the prioritization happens based on the message
	logging.debug(message)
	rand_int = random.randint(1, 10)
	logging.debug('generating a random score of: ' + str(rand_int))
	return rand_int

if __name__ == "__main__":

	logging.info('Started the work')
	parser = argparse.ArgumentParser()
	parser.add_argument("--test", help="run and generate dummy data to work on", action="store_true")

	args = parser.parse_args()

	if args.test:
		logging.debug("Using Test Work")
		time.sleep(5)  # work for a minute
		exit(0)  # exit successfully

	logging.debug("Using normal Work")
	exit(0)
