#
# Work Spawner 3000 code
#
import signal
import sys
import time
from subprocess import Popen
import logging
import argparse

#  Local modules
import WorkSpawnerConfig
import TopicReader
import PubSub

#  This is the module that contains all of the domain specific work.
import MyWork

# logging format is set in the WorkSpawnerConfig...this changes the level in this file.
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Spawner:

	def __init__(self):
		self.subprocess = None

	def pre_process(self, message):  # things that need to be done before processing work
		return MyWork.pre_process(message)

	def post_process(self, message):  # things that need to be done after the work is complete
		return MyWork.post_process(message)

	def get_work_cmd(self, message):
		return MyWork.get_work_cmd(message)

	def spawn_docker(self, docker_id, message):
		cmd = ['docker', 'run', '--rm', docker_id]
		logging.debug('Docker cmd: ' + str(cmd))
		self.subprocess = Popen(cmd)

	def spawn_shell(self, message):
		"""	payload: gets passed to the process"""
		cmd, cwd = self.get_work_cmd(message)

		logging.debug('shell cmd: ' + str(cmd))
		self.subprocess = Popen(cmd, cwd=cwd)  # default hook to start work.
		logging.info('spawned subprocess: ' + str(self.subprocess.pid))

	def is_spawn_done(self):
		rc = self.subprocess.poll()  # returns None if not done, else returns error code from subprocess
		if rc is None:  # process isn't done
			return False
		else:  # process completed and returned a return code
			exitcode = self.subprocess.terminate()
			if exitcode:  # even if successfully terminated, return an error due to time out
				raise Exception('subprocess returned an error code of: ' + str(exitcode))

		return True

	def wait(self, timeout):
		"""wait for a subprocess to be done or it times out
		:param timeout: number of seconds to wait for work to be done, otherwise stop. if zero, will wait forever
		:return: exitcode of the subprocess or -1 if timed out
		"""
		exitcode = 0
		if timeout:
			tracking_timeout = True
			timeout_ctr = timeout
		else:
			tracking_timeout = False
			timeout_ctr = 0

		process_done = False

		while not process_done:
			timeout_ctr -= 1  # decrement the timeout counter
			time.sleep(1)

			rc = self.subprocess.poll()  # returns None if not done, else returns error code from subprocess
			if rc is None:  # process isn't done
				continue  # the while loop
			else:  # process completed and returned a return code
				exitcode = rc
				process_done = True

			if tracking_timeout and timeout_ctr <= 0:
				exitcode = self.subprocess.terminate()
				if not exitcode:  # even if successfully terminated, return an error due to time out
					return -1

		return exitcode

	def terminate(self):
		self.subprocess.terminate()

def work_spawner():
	"""
	Look up work queues, pull work off highest queues down to lowest queues, invoke user specific work
	:return: none, will exit if errors out
	"""

	# Use instances so could parallel process in a future version
	spawner = Spawner()

	# get implementation specific instance
	queue = PubSub.PubSubFactory.get_queue()

	# function to call if the process gets killed or interrupted
	def signal_handler(sig, frame):
		logging.info('work_spawner is being terminated')
		sys.exit(0)

	# handle CTRL-C to stop subprocess
	signal.signal(signal.SIGINT, signal_handler)

	# interface to queue topics
	# reads in upon instantiation
	tr = TopicReader.Topics()
	if not tr:
		logging.error('No topics found')
		sys.exit(-1)

	index = 0  # index into the list of topics
	topics = tr.get_topic_list()

	while True:
		# TODO: always load the topics in case they have changed?
		# uses queue.ack() when don't want message processed again.  If this process gets killed before the
		# ack, the message will be available for another process

		if index >= len(topics):  # must have gone through all of the topics without finding work
			logging.info("No work found")
			time.sleep(10)  # if reached the end of the topics and there was no work, then sleep for a while
			index = 0  # reset the index for next time checking for work
			continue  # restart the while loop

		# Get the next topic from a list of topics
		topic = topics[index]
		logging.debug('Topic being checked: ' + topic)

		# synchronously pull one message at a time
		messages = queue.pull(topic, 1)

		if not messages:  # if there are no messages on that queue, move to next one.
			index += 1  # Move to lower priority topic if no message
			continue

		# If we got any messages, spawn a subprocess to handle each message in order received
		# then start over with the highest priority topic again
		for message in messages:  # loop through all of the messages and process each one
			logging.info('working with message: ' + str(message) + ' pulled from: ' + str(topic))

			# reset queue ack timeout.  that is how long pre_process has to finish
			queue.keep_alive(message)

			# perform any work that needs to be done before spawned. e.g., copying files etc.
			if not spawner.pre_process(message):
				logging.error('Could not pre_process message' + str(message))
				queue.log_failed_work(message)
				queue.ack(message)  # ack so that it is pulled off the queue so it won't be processed again
				continue  # for message loop

			# if there is a docker_id in the attributes, use it to spawn a docker file
			if 'docker_id' in message.attributes:
				docker_id = message.attributes['docker_id']
				# spawn as a sub process
				spawner.spawn_docker(docker_id, message)
			else:
				# spawn as a shell process
				spawner.spawn_shell(message)

			process_done = False
			timeout_ctr = WorkSpawnerConfig.WAIT_TIMEOUT
			start_time = time.time()

			while not process_done:
				# update so queue ack doesn't timeout
				queue.keep_alive(message)

				time_delta = time.time() - start_time
				timeout_ctr -= time_delta  # decrement the timeout counter

				if timeout_ctr <= 0:
					spawner.terminate()
					logging.error('worker timed out')
					queue.log_failed_work(message)
					queue.ack(message)  # ack so that it is pulled off the queue so it won't be processed again
					process_done = True
					continue

				try:
					process_done = spawner.is_spawn_done()
				except Exception as error:
					logging.error(error)

				time.sleep(5)  # how often to check the subprocess

			# wait for the subprocess to error or time out
			logging.info('work finished successfully')

			# reset queue ack timeout.  that is how long post_process has to finish
			queue.keep_alive(message)

			if not spawner.post_process(message):
				logging.error('Could not post_process message: ' + str(message))
				queue.log_failed_work(message)
				queue.ack(message)  # ack so that it is pulled off the queue so it won't be processed again
				continue  # for message loop

			queue.ack(message)  # acknowledge the message if successfully processed

		index = 0  # reset the index back to the highest priority queue so that work is always
					# pulled from there first


def work_prioritizer():
	"""
	Pull work from the "work to prioritize queue"
	Score it using a user defined function
	Look up the appropriate work queue using the score to find priority
	Put on on the work queu
	:return: None, will exit if error
	"""
	def signal_handler(sig, frame):
		logging.info('work_prioritizer is being terminated')
		sys.exit(0)

	# handle CTRL-C to stop subprocess
	signal.signal(signal.SIGINT, signal_handler)

	# instantiate the queue in interface
	queue = PubSub.PubSubFactory.get_queue()

	# topics are arranged highest to lowest
	tr = TopicReader.Topics()
	if not tr:
		logging.error('No topics found')
		exit(-1)

	# get the topic where work to be prioritized is queued
	priority_topic = tr.get_priority_topic()

	while True:
		# TODO: always load the topics in case they have changed?  wait until using memory cache

		# pull next work to prioritize
		logging.debug('Pulling work from priority_topic: ' + priority_topic)
		messages = queue.pull(priority_topic, 1)

		if not messages:  # if there are no messages on that queue, move to next one.
			logging.debug('no work found on prioritization queue')
			time.sleep(10)
			continue  # while loop

		# If we got any messages
		for message in messages:  # loop through all of the messages and process each one
			logging.debug('message: ' + str(message) + ' pulled from: ' + str(priority_topic))

			# use the message to extract a priority. This is done in the user specific MyWork.py.
			score = MyWork.prioritize(message)
			topic_to_publish_on = tr.get_topic(score)
			if topic_to_publish_on:
				logging.info('publishing: ' + str(message) + ' on topic: ' + str(topic_to_publish_on))
				queue.publish(topic_to_publish_on, message)
			else:
				logging.error('could not find a topic to send work to for score: ' + str(score))
				queue.log_failed_work(message)

			queue.ack(message)  # make sure it doesn't get processed again


if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("--spawner", help="run the work spawner daemon", action="store_true")
	parser.add_argument("--prioritizer", help="run the work prioritizer daemon", action="store_true")
	parser.add_argument("--test", help="put into debug mode and use test data", action="store_true")

	# get the args
	args = parser.parse_args()

	testing = args.test
	if testing:  # testing mode will generate and process fake data to test pub sub infrastructure
		logger = logging.getLogger()
		logger.setLevel(logging.DEBUG)
		logging.debug('In test mode')
		WorkSpawnerConfig.TEST_MODE = True  # set the global state

	if args.spawner:
		work_spawner()
	elif args.prioritizer:
		work_prioritizer()
	else:
		logging.error("Need to specify --spawner or --prioritizer")

