
import csv


def load_file(file_name="GCPTopics.csv"):
	rows = []
	with open(file_name, newline='') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			rows.append(row)
	return rows


def get_topic(rows, score):
	for row in rows:
		topic_root = row.get('Topic root')
		topic_id = row.get('topic uid')
		bin_low = float(row.get('low score'))
		bin_high = float(row.get('high score'))
		if bin_low <= score < bin_high:
			topic = topic_root + topic_id
			return(topic)

	return None

if __name__ == "__main__":

	rows = load_file()
#	print(rows)

	score = 2.7

	topic = get_topic(rows, score)
	print(topic)
