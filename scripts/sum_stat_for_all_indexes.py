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

def run(filename, bucket, pstat):
	f = open(filename, "r")
	i = 0
	l = f.readline()

	ll = l[53:]
	d = json.loads(ll)

	index_names = get_index_names_from_stats(d)

	sum = 0
	for iname in index_names:
		found_print = False
		ch = False
		pstat_key = bucket + ":" + iname + ":" + pstat
		pstat_val = d.get(pstat_key, -1)
		if pstat_val != -1:
			sum += int(pstat_val)

	print "Sum =", sum

if __name__ == '__main__':
	if len(sys.argv) != 4:
		print "Usage:\npython sum_stat_for_all_indexes.py <log-file-path> <bucket-name> <stat-name>"
		print "Expected file format: Single log line in file representing entire Periodic Stat log line (including timestamp)"
		os._exit(0)

	filename = sys.argv[1]
	bucket = sys.argv[2]
	pstat = sys.argv[3]

	print "filename:", filename
	print "bucket:", bucket
	print "pstat:", pstat
	run(filename, bucket, pstat)

