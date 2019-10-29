
import logging
import time
import argparse

import WorkSpawnerConfig
import WorkSpawner

# stateless re-entrant functions

def pre_process(message):  # things that need to be done before processing work
	logging.debug(message)


def get_work_cmd(message):  # default stub
	logging.debug(message)

	# unpack the payload and do any work that needs to be done
	cmd_to_run = ['python', 'MyWork.py', '--test']  # needs to be something Popen can run.
	return cmd_to_run

def post_process(message):  # things that need to be done after the work is complete
	logging.debug(message)

	# unpack the payload and do any work that needs to be done
	pass




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
