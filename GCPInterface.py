# place to store gcp related functions
# wrapper function that are needed to run a simulation on GCP (e.g., webserver requests etc)
# message queue interfaces for triggering functions from a message queue

'''
Requirements

get default bucket name
https://cloud.google.com/storage/docs/getting-bucket-information

get project name that the vm is running in
https://cloud.google.com/compute/docs/tutorials/python-guide


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
# how to use it in APP Engine
# https://cloud.google.com/appengine/docs/standard/python3/using-cloud-storage

# https://cloud.google.com/python/getting-started/using-cloud-storage
# https://cloud.google.com/python/tutorials/bookshelf-on-compute-engine


'''


import argparse
import logging
import os
from google.cloud import storage
import config


class GCSFile:

	def __init__(self):
		self.bucket = self.get_default_bucket()

	def list_buckets(self):
		"""Lists all buckets."""
		storage_client = storage.Client()
		buckets = storage_client.list_buckets()

		for bucket in buckets:
			print(bucket.name)

	def get_default_bucket(self):
		storage_client = storage.Client()
		bucket = storage_client.get_bucket(config.bucket_name)
		return bucket

	def create_file_for_writing(self, filename):
		"""Create a file.

		The retry_params specified in the open call will override the default
		retry params for this particular file handle.

		Args:
			filename: filename.
		"""
		logging.info('Creating file %s\n' % filename)

		gcs = self.bucket

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
		gcs = self.bucket

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
		gcs = self.bucket

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
		gcs = self.bucket

		self.response.write('Deleting files...\n')
		for filename in self.tmp_filenames_to_clean_up:
			self.response.write('Deleting file %s\n' % filename)
			try:
				gcs.delete(filename)
			except gcs.NotFoundError:
				pass

	def copy_local_to_bucket(self):
		def upload_blob(bucket_name, source_file_name, destination_blob_name):
			"""Uploads a file to the bucket."""
			storage_client = storage.Client()
			bucket = storage_client.get_bucket(bucket_name)
			blob = bucket.blob(destination_blob_name)

			blob.upload_from_filename(source_file_name)

			print('File {} uploaded to {}.'.format(
				source_file_name,
				destination_blob_name))
		pass

	def copy_bucket_to_local(self):
		def download_blob(bucket_name, source_blob_name, destination_file_name):
			"""Downloads a blob from the bucket."""
			storage_client = storage.Client()
			bucket = storage_client.get_bucket(bucket_name)
			blob = bucket.blob(source_blob_name)

			blob.download_to_filename(destination_file_name)

			print('Blob {} downloaded to {}.'.format(
				source_blob_name,
				destination_file_name))
		pass


class GCPubSub:
	def __init__(self):
		pass


	def publish_message(self, message):
		pass

	def get_message(self):
		message = 'default message'
		return message

class GCFunction:
	def __init__(self):
		pass


'''
https://docs.python.org/3/library/argparse.html
'''
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--nodisplay", help="run without drawing simulation on the screen", action="store_true")

	args = parser.parse_args()

	if args.nodisplay:
		print("no display")
	else:
		print("oh, I'm gonna display alright")

	test_gcs = GCSFile()
