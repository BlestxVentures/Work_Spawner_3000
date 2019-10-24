# place to store gcp related functions
# wrapper function that are needed to run a simulation on GCP (e.g., webserver requests etc)
# message queue interfaces for triggering functions from a message queue
# Tensorboard interface functions (move to a separate file)

'''
Requirements

get default bucket name
get project name that the vm is running in

copy files from local storage to gcs bucket
copy files from gcs bucket to local storage
https://cloud.google.com/storage/docs/renaming-copying-moving-objects#storage-rename-object-python
https://cloud.google.com/storage/docs/uploading-objects
https://cloud.google.com/storage/docs/downloading-objects#storage-download-object-python


score a set of work files so they can be prioritized on the queue
publish to a gcp topic
pull from a gcp topic
https://cloud.google.com/pubsub/docs/publisher


read from topic file and create topics if don't exist

'''

# how to use it in APP Engine
# https://cloud.google.com/appengine/docs/standard/python3/using-cloud-storage

# https://cloud.google.com/python/getting-started/using-cloud-storage
# https://cloud.google.com/python/tutorials/bookshelf-on-compute-engine


import logging
import os
import cloudstorage as gcs

from google.appengine.api import app_identity # to be able to get the app's current project id


class GCSFile:

	def __init__(self):
		bucket_name = self.get_gcs_filename()

	def get_gcs_filename(self):
		bucket_name = os.environ.get('BUCKET_NAME',
									 app_identity.get_default_gcs_bucket_name())

		# bucket_name = os.getenv('BUCKET_NAME',
		#                            app_identity.get_default_gcs_bucket_name())
		return bucket_name

	def create_file_for_writing(self, filename):
		"""Create a file.

		The retry_params specified in the open call will override the default
		retry params for this particular file handle.

		Args:
			filename: filename.
		"""
		logging.info('Creating file %s\n' % filename)

		write_retry_params = gcs.RetryParams(backoff_factor=1.1)
		gcs_file = gcs.open(filename,
							'w',
							content_type='text/plain',
							options={'x-goog-meta-foo': 'foo',
									 'x-goog-meta-bar': 'bar'},
							retry_params=write_retry_params)

		return gcs_file

	def write(self, string):

		self.write('abcde\n')
		self.write('f' * 1024 * 4 + '\n')

	def close(self):
		self.close()

	def read_file(self, filename):
		logging.info('Reading the full file contents:\n')

		gcs_file = gcs.open(filename)
		contents = gcs_file.read()
		gcs_file.close()
		return contents

	def list_bucket(self, bucket):
		"""Create several files and paginate through them.

		Production apps should set page_size to a practical value.

		Args:
			bucket: bucket.
		"""
		self.response.write('Listbucket result:\n')

		page_size = 1
		stats = gcs.listbucket(bucket + '/foo', max_keys=page_size)
		while True:
			count = 0
			for stat in stats:
				count += 1
				self.response.write(repr(stat))
				self.response.write('\n')

			if count != page_size or count == 0:
				break
			stats = gcs.listbucket(bucket + '/foo', max_keys=page_size,
								   marker=stat.filename)

	def delete_files(self):
		self.response.write('Deleting files...\n')
		for filename in self.tmp_filenames_to_clean_up:
			self.response.write('Deleting file %s\n' % filename)
			try:
				gcs.delete(filename)
			except gcs.NotFoundError:
				pass
