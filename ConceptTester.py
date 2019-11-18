import logging
import time

import WorkSpawnerConfig

# logging format is set in the WorkSpawnerConfig...this changes the level in this file.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


if __name__ == "__main__":

	logging.info("Using ConceptTester Work")

	log_file = './logs/file-' + str(time.time())
	with open(log_file, "a") as f:
		f.write("time 1: " + str(time.time()) + '\n')
		time.sleep(1)
		f.write("time 2: " + str(time.time()) + '\n')