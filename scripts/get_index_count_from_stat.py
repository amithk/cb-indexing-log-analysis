import json
import sys
import os

def get_index_names_from_stats(stats):
	index_names = {}
	for k in stats:
		if k.startswith(bucket):
			iname = k.split(":")[-2]
			if iname not in index_names:
				index_names[iname] = True

	return index_names.keys()

def run(filename, bucket):
	f = open(filename, "r")
	l = f.readline()
	ll = l[53:]
	d = json.loads(ll)

	index_names = get_index_names_from_stats(d)
	print len(index_names)


if __name__ == '__main__':
	if len(sys.argv) != 3:
		print "Usage:\npython analyse_stat_change_over_time.py <log-file-path> <bucket-name>"
		print "Expected file format: Single log line in file representing entire Periodic Stat log line (including timestamp)"
		os._exit(0)

	filename = sys.argv[1]
	bucket = sys.argv[2]

	print "filename:", filename
	print "bucket:", bucket
	run(filename, bucket)

